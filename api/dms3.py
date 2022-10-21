import pydantic 
from typing import Literal,Optional,Union,Tuple
import json
import typing
import re
import astropy.units as au
from rich.pretty import pprint

import pymongo
import bson
import builtins
import itertools

###
### SCHEMA
###

class ItemSchema(pydantic.BaseModel):
    'One data item in database schema'
    dtype: Literal['f','i','?','str','bytes','object']='f'
    unit: Optional[str]=None
    shape: pydantic.conlist(item_type=int,min_items=0,max_items=5)=[]
    link: Optional[str]=None

    @pydantic.validator("unit")
    def unit_valid(cls,v):
        if v is None: return
        try: au.Unit(v)
        except BaseException as e:
            print('Error validating unit {v} but exception not propagated??')
            raise
        return v

    @pydantic.root_validator
    def link_shape(cls,attrs):
        if attrs['link'] is not None:
            if len(attrs['shape'])>1: raise ValueError('links must be either scalar (shape=[]) or 1d array (shape=[num]).')
            if attrs['unit'] is not None: raise ValueError('unit not permitted with links')
        return attrs

class SchemaSchema(pydantic.BaseModel):
    'Schema of the schema itself; read via parse_obj'
    __root__: typing.Dict[str,typing.Dict[str,ItemSchema]]
    def getRoot(self): return self.__root__

    @pydantic.root_validator
    def links_valid(cls,attrs):
        root=attrs['__root__']
        for T,fields in root.items():
            for f,i in fields.items():
                if i.link is None: continue
                if i.link not in root.keys(): raise ValueError(f'{T}.{f}: link to undefined collection {i.link}.')
        return root




##
## VARIOUS HELPER FUNCTIONS
##
def _is_object_id(o):
    '''Desides whether *o* is string representation of bson.objectid.ObjectId'''
    return isinstance(o,str) and len(o)==24

def _apply_link(item,o,func):
    '''Applies *func* to scalar link or list link, and returns the result (as scalar or list, depending on the schema)'''
    assert item.link is not None
    assert len(item.shape) in (0,1)
    if len(item.shape)==1: return [func(obj=o_,index=ix) for ix,o_ in enumerate(o)]
    return func(obj=o,index=None)

from collections.abc import Iterable
def _flatten(items, ignore_types=(str, bytes)):
    '''Flattens possibly nested sequence'''
    for x in items:
        if isinstance(x, Iterable) and not isinstance(x, ignore_types): yield from _flatten(x, ignore_types)
        else: yield x

import numpy as np
import collections.abc as abc
Seq=abc.Sequence

@pydantic.validate_arguments()
def _validated_quantity_2(
        item: ItemSchema,
        value: Union[int,float,Seq[int],Seq[float],Seq[Seq[int]],Seq[Seq[float]],Seq[Seq[Seq[int]]],Seq[Seq[Seq[float]]],Seq[Seq[Seq[Seq[int]]]],Seq[Seq[Seq[Seq[float]]]]],
        unit: Optional[str]=None
    ):
    '''
    Converts value and optional unit to either np.array or astropy.units.Quantity — depending on whether the schema has unit or not. Checks 
    
    * dtype compatibility (won't accept floats/complex into an integer array)
    * dimension compatibility (will reject 2d array where schema specifies scalar or 1d array, and similar)
    * shape compatibility (will reject 4-vector where schema specified 3-vector; schema may use -1 in dimension where no check will be done; i.e. 3×? array has shape [3,-1])
    * unit compatibility: whether *unit* and schema unit are compatible; and will convert value to the schema unit before returning
    
    Returns np.array (no unit in the schema) or astropy.unit.Quantity (in schema units).
    '''
    assert item.link is None
    # 1. create np.array
    # 1a. check numeric type convertibility (must be done item-by-item; perhaps can be optimized later?)
    for it in _flatten(value):
        if not np.can_cast(it,item.dtype,casting='same_kind'): raise ValueError(f'Type mismatch: item {it} cannot be cast to dtype {cls.dtype} (using same_kind)')
    np_val=np.array(value,dtype=item.dtype)
    # 1b. check shape
    if len(item.shape) is not None:
        if len(item.shape)!=np_val.ndim: raise ValueError(f'Dimension mismatch: {np_val.ndim} (shape {np_val.shape}), should be {len(item.shape)} (shape {item.shape})')
        for d in range(np_val.ndim):
            if item.shape[d]>0 and np_val.shape[d]!=item.shape[d]: raise ValueError(f'Shape mismatch: axis {d}: {np_val.shape[d]} (should be {item.shape[d]})')
    # 2. handle units
    # 2a. schema has unit, data does not; or vice versa
    if (unit is None)!=(item.unit is None): raise ValueError(f'Unit mismatch: item {it} stored unit is {unit} but schema unit is {item.unit}')
    
    # 2b. no unit, return np_val only
    if item.unit is None: return np_val
    # 2c. has unit, convert to schema unit (will raise exception is units are not compatible) and return au.Quantity
    return (np_val*au.Unit(unit)).to(item.unit)
                     
def _validated_quantity(item: ItemSchema, data):
    '''
    Gets sequence (value only) or dict as {'value':..} or {'value':..,'unit':..};
    passes that to _validated_quantity_2, which will do the proper data check and conversions;
    returns validated quantity as either np.array or astropy.units.Quantity
    '''
    if isinstance(data,abc.Sequence): return _validated_quantity_2(item,data)
    elif isinstance(data,dict):
        if extras:=(data.keys()-{'value','unit'}):
            raise ValueError('Quantity has extra keywords: {", ".join(extras)} (only value, unit allowed).')
        return _validated_quantity_2(item,data['value'],data.get('unit',None))
    
def _parse_path(path: str) -> [(str,Optional[int])]:
    '''
    Parses path *p* in dot notation, returning list of [(stem,index),...], where index is possibly None. For example:
    
    dot[1].notation → [('dot',1),('notation',None)]
    '''
    if path=='' or path is None: return []
    pp=path.split('.')                      # split by ., dot may not appear inside [..] anyway
    pat=re.compile(r'''                  # no whitespace allowed in the expression
        (?P<stem>[a-zA-Z][a-zA-Z0-9_]*)  # stem: starts with letter, may continue with letters/numbers/_
        (\[(?P<index>[0-9]+)\])?         # optional index: decimals insides [...]
    ''',re.X)
    def _int_or_none(o): return None if o is None else int(o)
    def _match_part(p):
        match=pat.match(p)
        if match is None: raise ValueError(f'Failed to parse path {path} (component {p}).')
        return match['stem'],_int_or_none(match['index'])
    return [_match_part(p) for p in pp]
def _unparse_path(path: [(str,Optional[int])]):
    return '.'.join([stem+(f'[{index}]' if index is not None else '') for stem,index in path])
    
@pydantic.validate_arguments(config=dict(arbitrary_types_allowed=True))
def _quantity_to_dict(q: Union[np.ndarray,au.Quantity]) -> dict: 
    if isinstance(q,au.Quantity): return {'value':q.value.tolist(),'unit':str(q.unit)}
    return {'value':q.tolist()}



@pydantic.dataclasses.dataclass
class _ResolvedPath(object):
    obj: typing.Any
    type: str
    id: str
    tail: typing.List[Tuple[str,Optional[int]]]
    parent: Optional[str]

def _resolve_path_head(db: str,type: str, id: str, path: Optional[str]) -> _ResolvedPath:
    '''
    Resolves path head, descending as far as it can get, and returns (klass,dbId),path_tail.
    '''
    def _descend(klass,dbId,path,level,parentId):
        obj=GG.db_get(db)[klass].find_one({'_id':bson.objectid.ObjectId(dbId)})
        if len(path)==0: return _ResolvedPath(obj=obj,type=klass,id=dbId,tail=[],parent=None if level==0 else parentId)
        assert len(path)>0
        if obj is None: raise KeyError('No object {klass} with id={dbId} in the database')
        klassSchema=GG.schema_get_type(db,klass)
        attr,index=path[0]
        item=klassSchema[attr]
        if item.link is not None:
            if index is not None:
                if len(item.shape)==0: raise IndexError(f'{klass}.{attr} is scalar, but was indexed with {index}.')
                linkId=obj[attr][index]
            else:
                if len(item.shape)>0: raise IndexError(f'{klass}.{attr} is a list, but was not indexed.')
                linkId=obj[attr]
            if len(path)==1: return _ResolvedPath(obj=obj,type=item.link,id=linkId,tail=[],parent=parentId) # path leaf
            else: return _descend(klass=item.link,dbId=obj[attr][index],path=path[1:],level=level+1,parentId=dbId)
        else:
            return _ResolvedPath(obj=obj,type=klass,id=dbId,tail=path,parent=parentId) # ((klass,dbId),path)
    return _descend(klass=type,dbId=id,path=_parse_path(path),level=0,parentId=id)



from fastapi import FastAPI
app=FastAPI()

@app.get('/')
def root(): return 'ok'


##
## schema POST, GET
## 
@app.post('/{db}/schema')
def dms_api_schema_post(db: str, schema: str,force:bool=False):
    'Writes schema to the DB. TODO: also refresh the global GG.schema variable automatically?'
    coll=GG.db_get(db)['schema']
    if (s:=coll.find_one()) is not None and not force: raise ValueError('Schema already defined (use force=True if you are sure).')
    if s is not None: coll.delete_one(s)
    coll.insert_one(schema)

@app.get('/{db}/schema')
def dms_api_schema_get(db: str,include_id:bool=False):
    ret=GG.db_get(db)['schema'].find_one()
    if ret is None: raise KeyError(f'No schema defined in database {db}.')
    if ret is not None and not include_id: del ret['_id']
    return ret

@pydantic.dataclasses.dataclass
class _ObjectTracker:
    path2id: dict=pydantic.dataclasses.Field(default_factory=dict)
    id2path: dict=pydantic.dataclasses.Field(default_factory=dict)
    def add_tracked_object(self,path,id):
        self.path2id[tuple(path)]=id
        self.id2path[id]=tuple(path)
    def resolve_relpath_to_id(self,*,relpath,curr):
        tail=relpath
        where=curr
        while True:
            if tail.startswith('.'):
                tail=tail[1:]
                where=where[:-1]
                continue
            dot=tail.find('.')
            assert dot!=0
            if dot>0: head,tail=tail[:dot],tail[dot+1:]
            else: head,tail=tail,''
            if m:=re.match('^(.*)\[([0-9])+\]$',head): name,ix=m.groups(1),int(m.groups(2))
            else: name,ix=head,None
            where.append((name,ix))
            if tail=='': break
        if tuple(where) not in self.path2id: raise RuntimeError(f'Unable to resolve "{relpath}" relative to "{curr}" (known objects: {" ".join([_unparse_path(p) for p in self.path2id.keys()])}')
        return self.path2id[tuple(where)]
    def resolve_id_to_relpath(self,*,id,curr):
        abspath=self.id2path.get(id,None)
        if abspath is None: return None
        #print(f'{id=} {curr=} {abspath=}')
        for common in itertools.count(start=0):
            if curr[:common+1]!=abspath[:common+1]: break
        #print(f'{common=}')
        return (len(curr)-common-1)*'.'+_unparse_path(abspath[common:])



@app.post('/{db}/{type}')
def dms_api_object_post(db: str, type:str,data:dict) -> str:
    def _new_object(klass,dta,path,tracker):
        klassSchema=GG.schema_get_type(db,klass)
        rec=dict()
        meta=dta.pop('_meta',None)
        # only transfer selected metadata
        if meta is not None: rec['_meta']={'upstream':meta['_id']}
        for key,val in dta.items():
            if not key in klassSchema: raise AttributeError(f'Invalid attribute {klass}.{key} (hint: {klass} defines: {", ".join(klassKeys)}).')
            item=klassSchema[key]
            if item.link is not None:
                if len(item.shape)>0 and not isinstance(val,list): raise ValueError(f'{klass}.{key} should be list (not a {val.__class__.__name__}).')
                def _handle_link(*,obj,index,key=key,path=path):
                    if _is_object_id(obj): return obj
                    elif isinstance(obj,dict):
                        return _new_object(item.link,obj,path+[(key,index)],tracker)
                    elif isinstance(obj,str):
                        # relative path to an object already created, resolve it...
                        return tracker.resolve_relpath_to_id(relpath=obj,curr=path)
                    else: raise ValueError('{klass}.{key}: must be dict, object ID or relative path (not a {obj.__class__.__name__})')
                rec[key]=_apply_link(item,val,_handle_link)
            elif item.dtype=='str':
                if not isinstance(val,str): raise TypeError(f'{klass.key} must be str (not a {val.__class__.__name__})')
                rec[key]=val
            elif item.dtype=='bytes':
                if not isinstance(val,str): raise TypeError('{klass.key} must be a str (base64-encoded perhaps).')
                rec[key]=val
            elif item.dtype=='object':
                rec[key]=json.loads(json.dumps(val))
            else: # quantity
                q=_validated_quantity(item,val)
                rec[key]=_quantity_to_dict(q)
        ins=GG.db_get(db)[klass].insert_one(rec)
        idStr=str(ins.inserted_id)
        tracker.add_tracked_object(path,idStr)
        return idStr
    return _new_object(type,data,path=[],tracker=_ObjectTracker())

@app.get('/{db}/{type}/{id}/clone')
def dms_api_path_clone_get(db:str,type:str,id:str) -> str:
    dump=dms_api_path_get(db=db,type=type,id=id,path=None,max_level=-1,tracking=True,meta=True)
    return dms_api_object_post(db=db,type=type,data=dump)


@app.get('/{db}/{type}/{id}')
def dms_api_path_get(db:str,type:str,id:str,path: Optional[str]=None, max_level:int=-1, tracking=False, meta=True) -> dict:
    def _get_object(klass,dbId,parentId,path,tracker):
        if tracker and (p:=tracker.resolve_id_to_relpath(id=dbId,curr=path)): return p
        if max_level>=0 and len(path)>max_level: return {}
        obj=GG.db_get(db)[klass].find_one({'_id':bson.objectid.ObjectId(dbId)})
        assert str(obj['_id'])==dbId
        if obj is None: raise KeyError('No object {klass} with id={dbId} in the database.')
        klassSchema=GG.schema_get_type(db,klass)
        ret={}
        meta=ret['_meta']=obj.pop('_meta',{})
        meta|=dict(_id=str(obj.pop('_id')),type=klass)
        if parentId is not None: meta['parent']=parentId
        if not meta: ret.pop('_meta')
        for key,val in obj.items():
            if not key in klassSchema: raise AttributeError(f'Invalid stored attribute {klass}.{key} (not in schema).')
            item=klassSchema[key]
            if item.link is not None:
                if len(path)==max_level: continue
                def _resolve(*,obj,index,i=item,key=key): return _get_object(i.link,obj,parentId=dbId,path=path+[(key,index)],tracker=tracker)
                ret[key]=_apply_link(item,val,_resolve)
            else:
                ret[key]=val
        if tracker: tracker.add_tracked_object(path,dbId)
        return ret
    root=(type,id)
    R=_resolve_path_head(db=db,type=type,id=id,path=path)

    if len(R.tail)==0:
        obj=_get_object(R.type,R.id,parentId=R.parent,path=[],tracker=_ObjectTracker() if tracking else None)
        return obj

    # the result is an attribute which is yet to be obtained from the object
    if len(R.tail)>1: raise ValueError(f'Path {path} has too long tail ({_unparse_path(R.tail)}).')
    attr,index=R.tail[0]
    if index is not None: raise ValueError(f'Path {path} indexes an attribute (indexing is only allowed within link array)')
    item=GG.schema_get_type(db,R.type)[attr]
    assert item.link is None
    return R.obj[attr]


@app.get('/{db}')
def dms_api_type_list(db: str):
    return list(GG.schema_get(db).dict().keys())

@app.get('/{db}/{type}')
def dms_api_object_list(db: str, type: str):
    res=GG.db_get(db)[type].find()
    return [str(r['_id']) for r in res]



class GG(object):
    '''Global (static) objects for the server, used throughout. Populated at startup here below'''
    _DB={}
    _SCH={}
    _cli=None

    @staticmethod
    def client_set(cli: pymongo.MongoClient):
        GG._cli=cli
    @staticmethod
    def db_get(db:str):
        if db not in GG._DB: GG._DB[db]=GG._cli[db]
        return GG._DB[db]
    @staticmethod
    def schema_get(db:str):
        if db not in GG._SCH:
            rawSchema=dms_api_schema_get(db=db)
            if '_id' in rawSchema: del rawSchema['_id'] # this prevents breakage when reloading
            GG._SCH[db]=SchemaSchema.parse_obj(rawSchema)
        return GG._SCH[db]
    @staticmethod
    def schema_get_type(db:str,type:str):
        return getattr(GG.schema_get(db),type)

    @staticmethod
    def schema_invalidate_cache():
        GG._SCH={}

    @staticmethod
    def schema_import(db:str, json_str:str, force=False):
        rawSchema=json.loads(json_str)
        dms_api_schema_post(db,rawSchema,force=force)
        GG.schema_invalidate_cache()

    @staticmethod
    def schema_import_maybe(db: str, json_str:str):
        try: s=GG.schema_get(db)
        except KeyError:
             GG.schema_import(db,json_str)

GG.client_set(pymongo.MongoClient("localhost",27017))
GG.schema_import_maybe('dms0',open('dms-schema.json').read())



@app.exception_handler(Exception)
async def validation_exception_handler(request, err):
    from fastapi.responses import JSONResponse
    import traceback
    return JSONResponse(status_code=400, content={
        "type": type(err).__name__,
        "message": f"{err}".replace('"',"'"),
        "url": str(request.url),
        "method": str(request.method),
        "traceback": traceback.format_exc().replace('"',"'").split("\n"),
    })


if __name__=='__main__':
    import uvicorn
    uvicorn.run('dms3:app',host='0.0.0.0',port=8080,reload=True)


if 0:
    pprint(dms_api_schema_get(db='dms0'))
    print(schema)

    ##
    ## insert something into the DB
    ##
    CRVE_ID=dms_api_object_post('ConcreteRVE',
        {
            "origin":{"value":[1,2,3],"unit":"mm"},
            "size":{"value":[1,2,3],"unit":"km"},
            "materials":[
                {"name":"mat1","props":{"origin":"CZ"}},
                {"name":"mat2","props":{"origin":"DE"}}
            ],
             "ct":{"id":"bar","image":bytes(range(70,80))}
        }
    )
    print(CRVE_ID)
    
    
    print(_resolve_path_head(root=('ConcreteRVE',CRVE_ID),path='materials[0].name'))
    print(_resolve_path_head(root=('ConcreteRVE',CRVE_ID),path=''))
    pprint(dms_api_object_get(root=('ConcreteRVE',CRVE_ID),path='materials[1]',max_level=1))
    pprint(dms_api_attr_get(root=('ConcreteRVE',CRVE_ID),path='materials[1].name'))
    pprint(dms_api_attr_get(root=('ConcreteRVE',CRVE_ID),path='origin'))
    
    print(dms_api_type_list())
    for T in dms_api_type_list():
        print(T,dms_api_object_list(T))
