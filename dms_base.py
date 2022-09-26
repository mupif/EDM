import pydantic
from typing import Optional,Union,List
import numpy as np

import astropy.units as au
import astropy.constants

au.add_enabled_units([
    au.def_unit('none', au.dimensionless_unscaled)
])


from collections.abc import Iterable
def _flatten(items, ignore_types=(str, bytes)):
    for x in items:
        if isinstance(x, Iterable) and not isinstance(x, ignore_types): yield from _flatten(x, ignore_types)
        else: yield x

@pydantic.validate_arguments(config=dict(arbitrary_types_allowed=True))
def quant_field(shape: List[int]=[],dtype: Union[str,np.dtype]='f8', unit: Optional[str]=None):
    class QuantModel(pydantic.BaseModel):
        @classmethod
        def __get_validators__(cls): yield cls.validate
        @classmethod
        def validate(cls, val):
            # print(f'{cls=} {cls.shape=} {cls.unit=} {cls.dtype=} {val=}')
            if isinstance(val,(list,tuple)):
                for item in _flatten(val):
                    if not np.can_cast(item,cls.dtype,casting='safe'): raise ValueError(f'Type mismatch: item {item} cannot be safely cast to dtype {cls.dtype}')
                val=np.array(val,dtype=cls.dtype)
            else:
                if not np.can_cast(np.array(val),cls.dtype,casting='same_kind'): raise ValueError(f'Type mismatch: value of type {type(val)} could not be cast to dtype {cls.dtype}')
                # raise TypeError('value must be list,tuple or np.ndarray subclass')
            if cls.unit is None: ret=np.array(val,dtype=cls.dtype)
            else: ret=au.Quantity(val,dtype=cls.dtype)
            if len(cls.shape) is not None:
                if len(cls.shape)!=ret.ndim: raise ValueError(f'Dimension mismatch: {ret.ndim} (shape {ret.shape}), should be {len(cls.shape)} (shape {cls.shape})')
                for d in range(ret.ndim):
                    if cls.shape[d]>0 and ret.shape[d]!=cls.shape[d]: raise ValueError(f'Shape mismatch: axis {d}: {ret.shape[d]} (should be {cls.shape[d]})')
            if cls.unit is not None: ret=ret.to(cls.unit)
            return ret
    QM=QuantModel
    QM.shape=tuple(shape)
    QM.dtype=np.dtype(dtype)
    QM.unit=unit
    return QM

class DmsBaseModel(pydantic.BaseModel):
    class Config:
        def ndarray_save(arr):
            # inefficient but human-readable
            if isinstance(arr,au.Quantity): return {'dtype':str(arr.dtype),'data':arr.value.tolist(),'unit':str(arr.unit)}
            else: return {'dtype':str(arr.dtype),'data':arr.tolist()}
        # custom encoders
        json_encoders={np.ndarray: ndarray_save}

if __name__=='__main__':
    print(quant_field(shape=[-1,2]).validate([[1,2],[3,4]]))
