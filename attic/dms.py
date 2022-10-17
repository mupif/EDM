import pydantic
from typing import Optional,Union,List
import numpy as np
import io
import astropy.units as au
import astropy.constants
import beanie


au.add_enabled_units([
    # elementary charge is technically a constant
    au.def_unit('e', astropy.constants.si.e),
    # this is shorter
    au.def_unit('none', au.dimensionless_unscaled)
])


from collections.abc import Iterable
def _flatten(items, ignore_types=(str, bytes)):
    for x in items:
        if isinstance(x, Iterable) and not isinstance(x, ignore_types): yield from _flatten(x, ignore_types)
        else: yield x

def _shp(shape): return 'f[{"×".join(shape)}]'

@pydantic.validate_arguments(config=dict(arbitrary_types_allowed=True))
def quant_field(shape: List[int]=[],dtype: Union[str,np.dtype]='float64', unit: Optional[str]=None):
    class QuantModel(pydantic.BaseModel):
        @classmethod
        def __get_validators__(cls): yield cls.validate
        @classmethod
        def validate(cls, val):
            if isinstance(val,(list,tuple)):
                for item in _flatten(val):
                    if not np.can_cast(item,cls.dtype,casting='safe'): raise TypeError(f'Type mismatch: item {item} cannot be safely cast to dtype {cls.dtype}')
                val=np.array(val,dtype=cls.dtype)
            else:
                if not np.can_cast(np.array(val),cls.dtype,casting='safe'): raise TypeError(f'Type mismatch: value of type {type(val)} could not be cast to dtype {cls.dtype}')
                # raise TypeError('value must be list,tuple or np.ndarray subclass')
            if cls.unit is None: ret=np.array(val,dtype=cls.dtype)
            else: ret=au.Quantity(val,dtype=cls.dtype)
            if len(cls.shape) is not None:
                if len(cls.shape)!=ret.ndim: raise ValueError(f'Dimension mismatch: {ret.ndim} ({_shp(ret.shape)}), should be {len(cls.shape)}: {_shp(cls.shape)}')
                for d in range(ret.ndim):
                    if cls.shape[d]>0 and ret.shape[d]!=cls.shape[d]: raise ValueError(f'Shape mismatch (axis {d}): {_shp(ret.shape)}, should be {_shp(cls.shape)}')
            if cls.unit is not None: ret=ret.to(cls.unit)
            return ret
    QM=QuantModel
    QM.shape=tuple(shape)
    QM.dtype=np.dtype(dtype)
    QM.unit=unit
    return QM

def _ndarray_save(arr):
    # inefficient but human-readable
    if isinstance(arr,au.Quantity): return {'dtype':str(arr.dtype),'data':arr.value.tolist(),'unit':str(arr.unit)}
    else: return {'dtype':str(arr.dtype),'data':arr.tolist()}

class DmsModel(beanie.Document):
    # pydantic
    class Config:
        json_encoders={np.ndarray:_ndarray_save}
        extra='forbid'
    # beanie
    class Settings:
        bson_encoders={np.ndarray:_ndarray_save}

if __name__=='__main__':

    class A(DmsModel):
        arr22f_m: quant_field(shape=[2,2],unit='m')
        scalarf_kg: quant_field(shape=[],unit='kg')
        arr2x_none: quant_field(shape=[-1,2],dtype='i')
        arr22f_none: quant_field(shape=[2,2])=[[1,2,],[3,4]]
        raw_data: Optional[bytes]=None

    a=A(arr22f_m=[[1,2],[3,4]]*au.mm,scalarf_kg=3*au.mg,arr22f_none=[[0,1],[2,3]],arr2x_none=[[0,0],[1,1],[2,2],[3,3]],raw_data=bytes([0,1,2,3,4,5,6,7,8]))
    from rich.pretty import pprint as print
    # from rich import print_json
    import json
    print(a.dict())
    print('JSON:')
    print(json.loads(a.json()))
    # print_json(a.json())

# convert to prescribed unit (SQL queries, unit-ignorant)
