import pydantic 
from typing import Literal,Optional,Union
import json
import typing
import re
import astropy.units as au
from rich.pretty import pprint

import pymongo
import bson
import builtins


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
    if len(item.shape)==1: return [func(o_) for o_ in o]
    return func(o)

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
    if path=='': return []
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



def _resolve_path_head(root: (str,str), path: Optional[str]) -> ((str,str),str):
    '''
    Resolves path head, descending as far as it can get, and returns (klass,dbId),path_tail.
    '''
    def _descend(klass,dbId,path,level):
        if len(path)==0: return ((klass,dbId),None)
        obj=GG.db[klass].find_one({'_id':bson.objectid.ObjectId(dbId)})
        # print(f'{" "*level} {path=} {len(path)=} {obj=}')
        if obj is None: raise KeyError('No object {klass} with id={dbId} in the database')
        klassSchema=getattr(GG.schema,klass)
        attr,index=path[0]
        item=klassSchema[attr]
        if item.link is not None:
            if index is not None:
                if len(item.shape)==0: raise IndexError(f'{klass}.{attr} is scalar, but was indexed with {index}.')
                linkId=obj[attr][index]
            else:
                if len(item.shape)>0: raise IndexError(f'{klass}.{attr} is a list, but was not indexed.')
                linkId=obj[attr]
            if len(path)==1: return ((item.link,linkId),None) # path leaf
            else: return _descend(klass=item.link,dbId=obj[attr][index],path=path[1:],level=level+1)
        else:
            return ((klass,dbId),path)
    if path is None or path=='': return (root,None)       
    return _descend(root[0],root[1],path=_parse_path(path),level=0)



from fastapi import FastAPI
app=FastAPI()

@app.get('/')
def root(): return 'ok'


##
## schema POST, GET
## 
@app.post('/schema')
def dms_api_schema_post(schema: str,force:bool=False):
    'Writes schema to the DB. TODO: also refresh the global GG.schema variable automatically?'
    coll=GG.db['schema']
    if (s:=coll.find_one()) is not None and not force: raise ValueError('Schema already defined (use force=True if you are sure).')
    if s is not None: GG.db['schema'].delete_one(s)
    GG.db['schema'].insert_one(schema)

@app.get('/schema')
def dms_api_schema_get(include_id:bool=False):
    ret=GG.db['schema'].find_one()
    if ret is not None and not include_id: del ret['_id']
    return ret


@app.post('/{type}')
def dms_api_object_post(type:str,data:dict) -> str:
    def _new_object(klass,dta):
        klassSchema=getattr(GG.schema,klass)
        rec=dict()
        for key,val in dta.items():
            if not key in klassSchema: raise AttributeError(f'Invalid attribute {klass}.{key} (hint: {klass} defines: {", ".join(klassKeys)}).')
            item=klassSchema[key]
            if item.link is not None:
                rec[key]=_apply_link(item,val,lambda o: o if _is_object_id(o) else _new_object(item.link,o))
            elif item.dtype in ('str','bytes'):
                T={'str':str,'bytes':bytes}[item.dtype]
                if not isinstance(val,T): raise TypeError(f'{klass}.{key} must be a {item.dtype} (not a {val.__class__.__name__})')
                rec[key]=val
            elif item.dtype=='object':
                rec[key]=json.loads(json.dumps(val))
            else:
                # not a link, should validate and unit-convert data
                q=_validated_quantity(item,val)
                rec[key]=_quantity_to_dict(q)
        ins=GG.db[klass].insert_one(rec)
        return str(ins.inserted_id)
    return _new_object(type,data)

@app.get('/{type}/{id}/object')
def dms_api_object_get(type:str,id:str,path: Optional[str]=None, max_level:int=-1) -> dict:
    def _get_object(klass,dbId,level):
        if max_level>=0 and level>max_level: return {}
        obj=GG.db[klass].find_one({'_id':bson.objectid.ObjectId(dbId)})
        if obj is None: raise KeyError('No object {klass} with id={dbId} in the database.')
        klassSchema=getattr(GG.schema,klass)
        ret=dict()
        for key,val in obj.items():
            if key in ('_id',): continue
            if not key in klassSchema: raise AttributeError(f'Invalid stored attribute {klass}.{key} (not in schema).')
            item=klassSchema[key]
            if item.link is not None:
                if level==max_level: continue
                def _resolve(o,*,i=item,level=level): return _get_object(i.link,o,level=level+1)
                ret[key]=_apply_link(item,val,_resolve)
            else:
                ret[key]=val
        ret['_id']=dbId
        return ret
    root=(type,id)
    root2,path2=_resolve_path_head(root,path)
    if path2 is not None: raise ValueError(f'Path {path} does not lead to an object (tail: {_unparse_path(path2)}).')
    return _get_object(root2[0],root2[1],level=0)

@app.get('/{type}/{id}/attr')
def dms_api_attr_get(type:str, id:str, path:str) -> dict:
    root=(type,id)
    root2,path2=_resolve_path_head(root,path)
    if path2 is None or len(path2)==0: raise ValueError(f'Path {path} does leads to an object ({root2[0]}), not an attribute.')
    if len(path2)>1: raise ValueError(f'Path {path} has too long tail ({_unparse_path(path2)}).')
    if path2[0][1] is not None: raise ValueError(f'Path {path} has leaf index {path2[0][1]}.')
    klass,dbId=root2
    attr=path2[0][0]
    obj=GG.db[klass].find_one({'_id':bson.objectid.ObjectId(dbId)})
    if obj is None: raise KeyError(f'No object {klass} with id={dbId} in the database.')
    klassSchema=getattr(GG.schema,klass)
    item=klassSchema[attr]
    assert item.link is None
    return obj[attr]

@app.get('/ls')
def dms_api_type_list():
    return list(GG.schema.dict().keys())

@app.get('/{type}/ls')
def dms_api_object_list(type: str):
    res=GG.db[type].find()
    return [str(r['_id']) for r in res]



class GG(object):
    '''Global (static) objects for the server, used throughout. Populated at startup here below'''
    DB=None
    schema=None

##
## connect to the DB
##
GG.db=pymongo.MongoClient("localhost",27017).dms0
##
## insert schema into the DB (overwrite any existing)
##
import sys
rawSchema=dms_api_schema_get()
if rawSchema is None:
    rawSchema=json.loads(open('dms-schema.json').read())
    dms_api_schema_post(rawSchema,force=True)
    if '_id' in rawSchema: del rawSchema['_id'] # this prevents breakage when reloading
GG.schema=SchemaSchema.parse_obj(rawSchema)



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
    pprint(dms_api_schema_get())
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
