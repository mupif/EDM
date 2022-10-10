import astropy.units as au
import pydantic
from typing import Optional,Union,List
import numpy as np
import io
import sys
sys.path.append('.')
from dms_base import quant_field, DmsBaseModel

class A(DmsBaseModel):
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
