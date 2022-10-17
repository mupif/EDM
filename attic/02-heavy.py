schemasJson=open('02-heavyschema.json','r').read()
import mupif as mp
import numpy as np
u=mp.U
with mp.HeavyStruct(h5path='struct3.h5',mode='overwrite',schemaName='DMS01.BeamState',schemasJson=schemasJson) as beamStates:
    beamStates.resize(1)
    bs0=beamStates[0]
    bs0.beam_raw=0
    bs0.cs_raw=0
    bs0.npointz=1
    bs0.csState_raw=[0,1]

    beams=beamStates.beam
    beams.resize(1)
    beam0=beams[0]
    beam0.length=2*u.m
    beam0.height=20*u.cm
    beam0.density=3100*u['kg/m3']
    beam0.bc_0=[False,False,True]
    beam0.bc_1=[True,True,True]
    beam0.cs_raw=0
    beam0.csState_raw=[0,1]

    crossSections=beams.cs
    crossSections.resize(1)
    cs0=crossSections[0]
    cs0.rve_raw=0
    cs0.rvePosition=np.array([1,1,1],dtype='float32')*u.um

    crossSectionStates=beamStates.csState
    crossSectionStates.resize(1)
    css0=crossSectionStates[0]
    css0.rveStates_raw=np.array([0])
    css0.bendingMoment=10*u['kN*m']
    css0.kappa=.4
    css0.eps_axial=2*u['um/m']

    rveStates=crossSectionStates.rveStates
    rveStates.resize(1)
    rs0=rveStates[0]
    rs0.rve_raw=0
    rs0.sigmaHom=100*u.MPa
    rs0.epsHom=5*u['um/m']
    rs0.stiffness=50*u.GPa
    rs0.esp0hom=3*u['um/m']

    rves=crossSections.rve
    rves.resize(2)
    rve0=rves[0]
    rve0.ct_raw=0
    rve0.origin=np.array([.1,.1,.1],dtype='float32')*u.um
    rve0.size=np.array([.9,.9,.9],dtype='float32')*u.um
    rve0.materials_raw=[0]

    materials=rves.materials
    materials.resize(1)
    mat0=materials[0]
    mat0.name='CSH gel'
    mat0.props=dict(origin='CZ',date='2022-07-07')

    cts=rves.ct
    cts.resize(1)
    cts[0].image=b'binary-data-arbitrary-length'
    cts[0].id=0


    print(f'{beamStates[0].beam.cs.rve.ct.id=}')
    print(f'{beamStates[0].beam.length=}')
    print(f'{beamStates[0].csState[0].rveStates[0].rve.ct.id=}')
    print(f'{beamStates[0].csState[0].rveStates[0].rve.materials[0].name=}')


