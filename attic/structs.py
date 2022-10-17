from typing import *
# from pydbantic import Base, PrimaryKey, Unique

# from pydantic import BaseModel as Base
# from beanie import Document as Base

# from pydantic_mongo import AbstractRepository, ObjectIdField
# import mongomantic
# from mongomantic import BaseRepository, MongoDBModel
# Base=MongoDBModel
# mark class as "heavy" (immutable), not deep-copying
# kopírovat či nekopírovat? Link
# constraints on dimensions
# constraints on dimensionality

import sys, os.path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import dms
Base=dms.DmsModel
from dms import quant_field as Q

import numpy as np
import astropy.units as u
from astropy.units import Unit as U

import beanie

def L(s): return beanie.Link[s]
# def L(s): return s

class Beam(Base):
    cs: L('CrossSection')
    length: Q(unit='m')
    height: Q(unit='m')
    density: Q(unit='kg/m3')
    bc_0: List[bool]
    bc_1: List[bool]

class CrossSection(Base):
    rve: L('ConcreteRVE')
    rvePositions: Q(shape=[-1,3],unit='um') # mp.Quantity

class ConcreteRVE(Base):
    ct: L('CTScan')
    origin: Q(shape=[3],unit='um')
    size: Q(shape=[3],unit='um')
    # discretizedMicrostructure: mp.Mesh
    materials: List['MaterialRecord']

class CTScan(Base):
    id_: int=-1
    image: bytes

class BeamState(Base):
    beam: L('Beam')
    cs: L('CrossSection')
    npointz: int
    csState: List[L('CrossSectionState')]

class CrossSectionState(Base):
    rveStates: List[L('ConcreteRVEState')]
    bendingMoment: Q(unit='kN*m')
    kappa: Q()
    eps_axial: Q(unit='um/m')

class ConcreteRVEState(Base):
    rve: L('ConcreteRVE')
    sigmaHom: Q(unit='MPa')
    epsHom: Q(unit='um/m')
    stiffness: Q(unit='MPa')
    eps0hom: Q(unit='um/m')

class MaterialRecord(Base):
    name: str
    props: dict

__all__=[Beam,CrossSection,ConcreteRVE,CTScan,BeamState,CrossSectionState,ConcreteRVEState,MaterialRecord]

for c in __all__:
    c.update_forward_refs()

async def main():
    import motor
    import beanie
    client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://localhost:27017')
    from rich.pretty import pprint as print
    print(__all__)
    await beanie.init_beanie(database=client.DMS01,document_models=__all__)


    rve=ConcreteRVE(
        ct=CTScan(image=b'1234534baysdfvasldlsd',id_=2345),
        origin=(0,0,0)*u.mm,
        size=(10,10,10)*u.nm,
        materials=[MaterialRecord(name='CSH',props={'foo':'bar'})]
    )
    beam=Beam(
        cs=CrossSection(rve=rve,rvePositions=np.array([[1,1,1]])*u.mm),
        length=1*u.m,
        height=20*u.cm,
        density=2400*U('kg/m3'),
        bc_0=[True,True,True],
        bc_1=[False,True,True]
    )
    beamState=BeamState(
        beam=beam,
        cs=beam.cs,
        npointz=1,
        csState=[
            CrossSectionState(
                rveStates=[
                    ConcreteRVEState(rve=rve,sigmaHom=.1*u.MPa,epsHom=.3,stiffness=10*u.MPa,eps0hom=1e-6),
                    ConcreteRVEState(rve=rve,sigmaHom=.2*u.MPa,epsHom=.6,stiffness=15*u.MPa,eps0hom=2e-6),
                ],
                bendingMoment=10*U('kN*m'),
                kappa=.1,
                eps_axial=1e-4
            )
        ]
    )

    beamStates=[beamState]

    # for i in [mat,ct,rve,rves,css,cs,beam,bs]: await i.insert()
    # await beamState.insert()
    # for i in [rve,beam,beamState]: await i.insert()
    #await rve.ct.insert()
    #await rve.materials[0].insert()
    #await rve.insert()
    #await beam.insert()
    await beamState.insert()


    print(f'{beamStates[0].beam.length=}')
    print(f'{beamStates[0].beam.cs.rve.ct.id_=}')
    print(f'{beamStates[0].csState[0].rveStates[1].rve.ct.id_=}')
    print(f'{beamStates[0].csState[0].rveStates[1].rve.materials[0].name=}')


if __name__=='__main__':
    import asyncio
    asyncio.run(main())
