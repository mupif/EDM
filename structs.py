from typing import *
# from pydbantic import Base, PrimaryKey, Unique

# from pydantic import BaseModel as Base
from beanie import Document as Base

# from pydantic_mongo import AbstractRepository, ObjectIdField
# import mongomantic
# from mongomantic import BaseRepository, MongoDBModel
# Base=MongoDBModel



# mark class as "heavy" (immutable), not deep-copying
# kopírovat či nekopírovat? Link

# constraints on dimensions
# constraints on dimensionality

import mupif as mp
from mupif import U as u
import numpy as np

class Beam(Base):
    cs: 'CrossSection'
    length: float # mp.Quantity
    height: float # mp.Quantity
    density: float # mp.Quantity
    bc_0: List[bool]
    bc_1: List[bool]

class CrossSection(Base):
    rve: 'ConcreteRVE'
    rvePositions: List[float] # mp.Quantity

class ConcreteRVE(Base):
    ct: 'CTScan'
    origin: List[float] #mp.Quantity
    size: List[float] #mp.Quantity
    # discretizedMicrostructure: mp.Mesh
    materials: List['MaterialRecord']

class CTScan(Base):
    id_: int
    image: bytes

class BeamState(Base):
    beam: 'Beam'
    cs: 'CrossSection'
    npointz: int
    csState: List['CrossSectionState']

class CrossSectionState(Base):
    rveStates: List['ConcreteRVEState']
    bendingMoment: float # mp.Quantity
    kappa: float # mp.Quantity
    eps_axial: float #mp.Quantity

class ConcreteRVEState(Base):
    rve: ConcreteRVE
    sigmaHom: float # mp.Quantity
    epsHom: float # mp.Quantity
    stiffness: float # mp.Quantity
    eps0hom: float # mp.Quantity

class MaterialRecord(Base):
    name: str
    props: dict

for c in [Beam,CrossSection,ConcreteRVE,CTScan,BeamState,CrossSectionState,ConcreteRVEState,MaterialRecord]:
    c.update_forward_refs()

if 0:
    rve=ConcreteRVE(
        ct=CTScan(image='1234534baysdfvasldlsd',id=2345),
        origin=(0,0,0)*u.mm,
        size=(10,10,10)*u.nm,
        materials=[MaterialRecord(name='CSH',props={'foo':'bar'})]
    )
    beam=Beam(
        cs=CrossSection(rve=rve,rvePositions=np.array([1,1,1])*u.mm),
        length=1*u.m,
        height=20*u.cm,
        density=2400*u['kg/m3'],
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
                    ConcreteRVEState(rve=rve,sigmaHom=.1*u.none,epsHom=.3*u.none,stiffness=10*u.MPa,eps0hom=1e-6*u.none),
                    ConcreteRVEState(rve=rve,sigmaHom=.2*u.none,epsHom=.6*u.none,stiffness=15*u.MPa,eps0hom=2e-6*u.none),
                ],
                bendingMoment=10*u['kN*m'],
                kappa=.1*u.none,
                eps_axial=1e-4*u.none
            )
        ]
    )

    beamStates=[beamState]

    print(f'{beamStates[0].beam.length=}')
    print(f'{beamStates[0].beam.cs.rve.ct.id=}')
    print(f'{beamStates[0].csState[0].rveStates[1].rve.ct.id=}')
    print(f'{beamStates[0].csState[0].rveStates[1].rve.materials[0].name=}')














# from pydantic_mongo import AbstractRepository, ObjectIdField
# from pymongo import MongoClient

#class BeamStatesRepo(BaseRepository):
#    class Meta:
#        model='BeamState'
#        # collection_name = 'BeamStates'
#        collection='beamState'
#
#mongomatic.connect('mongodb://127.0.0.1:27017','DMS00-mongomatic')
#print(f'{beamState.id=}')
#BeamStatesRepo.save(beamState)
