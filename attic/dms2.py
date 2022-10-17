import pydantic
import astropy.units as au
from typing import Literal,Optional
import json
from rich.pretty import pprint as pprint
from rich import print_json
import typing
import sys
import os
import logging
import weakref
import enum
import contextlib
logging.basicConfig()
log=logging.getLogger()

sys.path.append('.')
import dms_base

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

class DmsFileBackend(object):
    def __init__(self,root='db-dms'):
        self.root=root
    def _fn(self,coll,index): return f'{self.root}/{coll}/{index}.json'
    def doc_save(self,data,coll,index=None):
        # print(f'{self=} {coll=} {index=}')
        if index is None:
            # write new copy
            index=0
            while os.path.exists(f:=self._fn(coll,index)): index+=1
            os.makedirs(os.path.dirname(f),exist_ok=True)
            # json.dump(data,open(f,'w'))
            open(f,'w').write(data.json())
            return index
        else:
            # modify in-place
            open(f,'w').write(data.json())
            return None
    def doc_load(self,coll,index):
        return json.load(open(self._fn(coll,index)))
    def doc_delete(self,coll,index):
        os.remove(self._fn(coll,index))
    def coll_list(self,coll):
        ff=glob.glob('{self.root}/{coll}/[0-9]*.json')
        return sorted([int(f=os.path.splitext(f)[0]) for f in ff if f.isnumeric()])

class MongodbBackend(object):
    def __init__(self,db):
        self.db=db
        import bson.objectid
        self._OID=bson.objectid.ObjectId
    def doc_save(self,data,coll,index=None):
        if index is None:
            # pprint(data.dict())
            # print_json(data.json())
            res=self.db[coll].insert_one(json.loads(data.json()))
            return str(res.inserted_id)
        else:
            self.db[coll].update_one(data.dict())
    def doc_load(self,coll,index):
        res=self.db[coll].find_one(filter=self._OID(index))
        if res is None: raise ValueError(f'No document {coll=}, {index=}')
        # _id is in-band, which we don't want
        if '_id' in res:
            assert str(res['_id'])==index
            del res['_id']
        return res
    def doc_delete(self,coll,index):
        self.db[coll].delete_one(filter=self._OID(index))
    def coll_list(self,coll):
        return sorted(self.db[coll].find(filter=None,return_key=True))

raw=json.loads(open('dms-schema.json').read())
schema=SchemaSchema.parse_obj(raw)

class DbContext(pydantic.BaseModel):
    class Writing(enum.Enum):
        NEVER_DIRTY=0 # this is true only at construction time — allows any modifications to attribute
        IN_PLACE=1 # modifies data and writes them back
        COPY_ON_WRITE=1 # writes duplicate object and all its parents
        LOCKED=2
        FROZEN=3

    klassMap: typing.Dict[str,type]=pydantic.Field(...,exclude=True)
    conn: typing.Any=None
    index: int=-1
    path: str='' # purely informative, for debug messages
    dirty: bool=True
    writing: Writing=Writing.NEVER_DIRTY


    def _copy(self,subpath):
        return DbContext(klassMap=self.klassMap,conn=self.conn,path=self.path+'.'+subpath)
    def __repr_args__(self): return [(k,getattr(self,k)) for k in ('conn','index','path','writing','dirty')]
    def load(self,coll,index,path):
        data=self.conn.doc_load(coll,index)
        ctx=self._copy(subpath='')
        ctx.path=path
        ctx.index=index
        ctx.dirty=False
        print(f'Loading {coll=}, {index=}: {data=}')
        return self.klassMap[coll](ctx=ctx,parent=None,**data)


class DmsData(dms_base.DmsBaseModel):
    __root__: typing.Dict[str,typing.Any]=pydantic.Field(default_factory=dict)
    def __iter__(self): return iter(self.__root__)
    def __getitem__(self, item): return self.__root__[item]
    def __setitem__(self, item, value): self.__root__[item]=value

@pydantic.dataclasses.dataclass
class DocBase:
    ctx: typing.Optional[DbContext]=None
    data: DmsData=pydantic.Field(default_factory=DmsData)
    parent: typing.Any=None

    def set_children_parents(self):
        for attr,item in self._db_fields.items():
            if item.link is None: continue
            def _obj_set_parent(o,parent):
                if isinstance(o,DocBase): o.parent=parent
            if len(item.shape)==0: _obj_set_parent(self.data[attr],parent=self)
            else:
                for o in self.data[attr]:
                    _obj_set_parent(o,parent=self)
    def is_dirty(self): return not self.ctx or self.ctx.dirty
    def set_dirty(self):
        # print(f' XX {self.__class__.__name__} {self.ctx.path=}')
        if not self.ctx: return
        if self.ctx.writing==DbContext.Writing.NEVER_DIRTY: return
        if self.ctx.writing in (DbContext.Writing.FROZEN,DbContext.Writing.LOCKED): raise RuntimeError('Frozen or locked object (programming error; this should not happen).')
        self.ctx.dirty=True
        if self.ctx.writing==DbContext.Writing.COPY_ON_WRITE:
            print(f'{self.ctx.path}: COPY_ON_WRITE, {self.parent=}')
            if self.parent and (p:=self.parent): p.detach_child(self)
            self.ctx.index=None
            if self.parent and (p:=self.parent): p.set_dirty()

    def assert_writeable(self):
        if not self.ctx: return
        if self.ctx.writing==DbContext.Writing.FROZEN: raise RuntimeError(f'{self.ctx.path}: frozen object, writing impossible.')
        if self.ctx.writing==DbContext.Writing.LOCKED: raise RuntimeError(f'{self.ctx.path}: locked object, call unlock() to allow copy-on-write.')
        if self.ctx.writing in (DbContext.Writing.NEVER_DIRTY,DbContext.Writing.IN_PLACE,DbContext.Writing.COPY_ON_WRITE): return
        raise RuntimeError(f'{self.ctx.path}: unhandled value for {self.ctx.writing=}')
    def detach_child(self,child):
        # find child in links (single or list)
        if child.ctx is None: return
        coll=child.__class__.__name__
        if child.ctx.index is None: return
        #print(f'Trying to detach {coll=} {id(child)=}')
        found=False
        for attr,item in self._db_fields.items():
            if item.link!=coll: continue
            # print(f'  got collection {coll} for {child=}')
            if len(item.shape)==0:
                if self.data[attr]==child.ctx.index:
                    self.data[attr]=child
                    found=True
            else:
                dd=[]
                for i,ix in enumerate(self.data[attr]):
                    if ix==child.ctx.index:
                        dd.append(child)
                        found=True
                    else: dd.append(ix)
                self.data[attr]=dd
                # self.data[attr]=[(child if ix==child.ctx.index else ix for ix in self.data[attr])]
        # if not found: raise RuntimeError(f'Child object {child=} not found in links of {self=}')
        # if found: print('    → DETACHED ✓')
        self.set_dirty()
    def fetch_link(self,key):
        if self.ctx is None: return self.data[key]
        item=self._db_fields[key]
        coll=item.link
        klass=self.ctx.klassMap[coll]
        def _mk_obj(lnk,ix=None):
            if isinstance(lnk,DocBase): return lnk
            data=self.ctx.conn.doc_load(coll,lnk)
            # print(f'**fetch_link: {key=} {coll=} {lnk=}: {data=}')
            ctx=self.ctx._copy(subpath=key+('' if ix is None else f'[{ix}]'))
            ctx.dirty=False
            ctx.index=lnk
            ret=klass(ctx=ctx,parent=self,**data)
            ctx.writing=self.ctx.writing
            return ret
        if len(item.shape)==0: return _mk_obj(self.data[key])
        else: return [_mk_obj(i,ix) for ix,i in enumerate(self.data[key])]
    def save_link(self,*,key,scalar,coll,value):
        # if self.ctx is not None and self.ctx.writing!=DbContext.Writing.NEVER_DIRTY:
        #if not self.ctx or self.ctx.writing==DbContext.Writing.NEVER_DIRTY:
        #    self.data[key]=value
        #    return
        if self.ctx is None:
            self.data[key]=value
            return
        #print(f'save_link {key=} {coll=} {value=}')
        def _obj_id(obj,ix=None,*,key=key,coll=coll):
            if isinstance(obj,DocBase):
                if not obj.is_dirty(): return obj
                c2=self.ctx._copy(subpath=key)
                # print(f'  →  {key}{("["+str(ix)+"]") if (ix is not None) else ""}: {obj=}')
                obj.set_ctx(c2,overwrite=True)
                return obj.ctx.index
                #cc.append(child.ctx.index)
                #print(f'  →  {key}[{i}]: {child=}')
                #if obj.ctx.index is None: return 
        # saving to DB context, all objects must be either indices or
        #print(f'  {key} {coll} {value=} ')
        #print(f'     {self.data.dict()=}')
        if scalar: self.data[key]=_obj_id(value)
        else: self.data[key]=[_obj_id(v,ix=ix) for ix,v in enumerate(value)]

    @contextlib.contextmanager
    def _modify(self):
        self.assert_writeable()
        yield
        self.set_dirty()


    @contextlib.contextmanager
    def edit_clone(self):
        if self.ctx is None: raise RuntimError(f'{self.ctx.path}: no context.')
        if self.ctx.writing!=DbContext.Writing.LOCKED: raise RuntimeError(f'{self.ctx.path}: must be locked (not {self.ctx.writing})')
        self.ctx.writing=DbContext.Writing.COPY_ON_WRITE
        yield self
        self.flush()

    @contextlib.contextmanager
    def edit_inplace(self):
        if self.ctx is None: raise RuntimError(f'{self.ctx.path}: no context.')
        if self.ctx.writing!=DbContext.Writing.LOCKED: raise RuntimeError(f'{self.ctx.path}: must be locked (not {self.ctx.writing})')
        self.ctx.writing=DbContext.Writing.IN_PLACE
        yield
        self.flush()


    def flush(self):
        if not self.ctx: raise RuntimeError('No database context, nowhere to flush.')
        if not self.ctx.dirty: raise RuntimeError('Data clean, nothing to flush?')
        pprint(self.ctx.path)
        pprint(self.data)
        # traverse children
        table=self.__class__.__name__
        for key,item in self._db_fields.items():
            if item.link is None: continue # data member, nothing to do
            self.save_link(key=key,scalar=(len(item.shape)==0),coll=item.link,value=self.data[key])
        # actually save data here
        print(f'  (2) {self.data=}')
        self.ctx.index=self.ctx.conn.doc_save(self.data,table)
        self.ctx.dirty=False
        if self.ctx.writing in (DbContext.Writing.IN_PLACE,DbContext.Writing.COPY_ON_WRITE,DbContext.Writing.NEVER_DIRTY): self.ctx.writing=DbContext.Writing.LOCKED
        # print(f'Flushed {self.__class__.__name__} {id(self)=}')


    def set_ctx(self,ctx,overwrite=False):
        assert (self.ctx is None) or overwrite
        self.ctx=ctx
        # assert not self.ctx.table or overwrite
        # assert self.ctx.index is None
        self.flush()

    def __del__(self):
        if not self.ctx: return
        if self.ctx.index is None:
            # log.warning(f'{self.__class__.__name__} {id(self)=}')
            log.warning(f'Unsaved document {self.ctx.path} being destroyed: {self.__class__.__name__}, {id(self)=}\n{self.data=}\n{str(self)}.')

klassMap={}

for klass in schema.dict().keys():
    kAttrs=getattr(schema,klass).keys()
    kvAttrs=getattr(schema,klass).items()
    meth={}
    for key,item in kvAttrs:
        if item.link is not None:
            def link_getter(self,*,key=key,item=item):
                return self.fetch_link(key)
            def link_setter(self,val,*,key=key,item=item):
                self.assert_writeable()
                self.save_link(key=key,scalar=(len(item.shape)==0),coll=item.link,value=val)
                self.set_dirty()
            getset=(link_getter,link_setter)
        elif item.dtype in ('f','i','?'):
            F=dms_base.quant_field(shape=item.shape,dtype=item.dtype,unit=item.unit)
            def np_getter(self,*,key=key):
                ret=self.data[key].view()
                ret.flags.writeable=False
                return ret
            def np_setter(self,val,*,key=key,F=F):
                with self._modify():
                    self.data[key]=F.validate(val)
            getset=(np_getter,np_setter)
        elif item.dtype in ('str','bytes'):
            def strbytes_getter(self,*,key=key): return self.data[key]
            def strbytes_setter(self,val,*,key=key,item=item):
                assert isinstance(val,{'str':str,'bytes':bytes}[item.dtype])
                self.assert_writeable()
                # print(f'strbytes: {key=} {val=}')
                self.data[key]=val
                self.set_dirty()
            getset=(strbytes_getter,strbytes_setter)
        elif item.dtype=='object':
            def json_setter(self,val,*,key=key,item=item):
                self.assert_writeable()
                # print(f'object: {key=}')
                self.data[key]=json.loads(json.dumps(val))
                self.set_dirty()
            def json_getter(self,*,key=key,item=item): self.data[key] # json.loads(self.data[key])
            getset=(json_getter,json_setter)
        else: raise RuntimeError('Should be unreachable')
        meth[key]=property(fget=getset[0],fset=getset[1])
    def T_init(self,*,__attrs=set(kAttrs),__schema=schema,__klass=klass,**kw):
        if (missing:=(set(__attrs)-set(kw.keys()))): raise RuntimeError(f'{__klass}: some attributes not given: {", ".join(missing)}')
        later={}
        for k in __attrs: later[k]=kw.pop(k)
        DocBase.__init__(self,**kw)
        assert(self.ctx is None or self.ctx.writing==DbContext.Writing.NEVER_DIRTY)
        for k,v in later.items(): setattr(self,k,v)
        if self.ctx: self.ctx.writing=DbContext.Writing.LOCKED
        self.set_children_parents()
    meth['__init__']=T_init
    meth['_db_fields']=dict(kvAttrs)
    # print(f'{meth["_db_fields"]=}')
    T=type(klass,(DocBase,),meth)
    klassMap[klass]=T

ConcreteRVE=klassMap['ConcreteRVE']
CTScan=klassMap['CTScan']
MaterialRecord=klassMap['MaterialRecord']


if __name__=='__main__':
    import pymongo

    for backend in [DmsFileBackend(root='./db-dms'),MongodbBackend(db=pymongo.MongoClient("localhost",27017).dms)]:
        ctx=DbContext(klassMap=klassMap,conn=backend,path='rve')

        rve=ConcreteRVE(
            origin=[1,2,3]*au.m,
            size=[1,1,1]*au.mm,
            materials=[mat:=MaterialRecord(name='foo',props={'origin':'CZ'})],
            ct=CTScan(id='bar',image=bytes(range(70,80))),
        )

        rve.set_ctx(ctx)

        with rve.edit_clone():
            rve.materials[0].name='foo2'

        print(rve)

        print(f'{rve.ctx.index=}')
        print(100*'=')

        m=rve.materials[0]
        print(100*'-')

        with rve.edit_inplace():
            m=rve.materials[0]
            m.name='foo2'
        print(m)

        print(100*'@')
        rve2=ctx.load(rve.__class__.__name__,rve.ctx.index,path='rve2')
        print(rve2)

        with rve2.edit_inplace():
            rve2.origin=[5,5,5]*au.km
            print(f'{rve2.ctx.index=} {rve2.is_dirty()=}')

        # del cr2
        #print(cr2)
        # cr.flush()


        #print(f' !!! {cr.materials[0].name=}')
        #cr.flush()

        # unlock (detach) / lock
