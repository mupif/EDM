import sys
sys.path.append('.')
import structs as S
import beanie
import asyncio
import motor

async def main():
    client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://localhost:27017')

    await beanie.init_beanie(
        database=client.db_name,
        document_models=[
            S.Beam,
            S.CrossSection,
            S.ConcreteRVE,
            S.CTScan,
            S.BeamState,
            S.CrossSectionState,
            S.ConcreteRVEState,
            S.MaterialRecord
        ]
    )

    mat=S.MaterialRecord(name='CSH gel',props={'origin':'CZ','date':'2022-07-07'})
    ct=S.CTScan(image='binary-data',id_=1)
    rve=S.ConcreteRVE(ct=ct,origin=(.1,.1,.1),size=(.9,.9,.9),materials=[mat])
    rves=S.ConcreteRVEState(rve=rve,sigmaHom=100e9,epsHom=5e-6,stiffness=50e9,eps0hom=3e-6)
    css=S.CrossSectionState(rveStates=[rves],bendingMoment=10e3,kappa=.4,eps_axial=2e-6)
    cs=S.CrossSection(rve=rve,rvePositions=(1e-6,1e-6,1e-6))
    beam=S.Beam(length=2,height=0.2,density=2100,bc_0=[False,False,False],bc_1=[True,True,True],cs=cs,csState=css)
    bs=S.BeamState(beam=beam,cs=beam.cs,npointz=1,csState=[css])


    for i in [mat,ct,rve,rves,css,cs,beam,bs]: await i.insert()
    # chocolate = Category(name="Chocolate", description="A preparation of roasted and ground cacao seeds.")
    # Beanie documents work just like pydantic models
    #tonybar = Product(name="Tony's", price=5.95, category=chocolate)
    # And can be inserted into the database
    # await tonybar.insert() 
    # You can find documents with pythonic syntax
    #product = await Product.find_one(Product.price < 10)
    # And update them
    #await product.set({Product.name:"Gold bar"})
    beamStates=[bs]
    print(f'{beamStates[0].beam.length=}')
    print(f'{beamStates[0].beam.cs.rve.ct.id=}')
    print(f'{beamStates[0].csState[0].rveStates[0].rve.ct.id=}')
    print(f'{beamStates[0].csState[0].rveStates[0].rve.materials[0].name=}')


asyncio.run(main())
