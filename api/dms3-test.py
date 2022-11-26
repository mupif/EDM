import unittest
import requests
import json
from rich.pretty import pprint
from rich import print_json



# location of the DB (B=base URL for MongoDB, DB=database name)
B='http://localhost:8080'
DB='dms0'

# small wrapper for the REST API
def get(p,show=False,**kw):
    r=requests.get(f'{B}/{p}',params=kw)
    if not r.ok: raise RuntimeError(r.text)
    data=json.loads(r.text)
    if  show: pprint(data)
    return data
def post(p,**kw):
    r=requests.post(f'{B}/{p}',json=kw)
    if not r.ok: raise RuntimeError(r.text)
    return json.loads(r.text)
def patch(p,**kw):
    r=requests.patch(f'{B}/{p}',json=kw)
    if not r.ok: raise RuntimeError(r.text)
    return json.loads(r.text)



dta={ # BeamState 
    "beam":{ # Beam
        "length": { "value": 2500, "unit":"mm" },
        "height": { "value": 20, "unit":"cm" },
        "density": { "value": 3.5, "unit":"g/cm3" },
        "cs":{ # CrossSection
            "rvePositions": {"value":[[1,2,3],[4,5,6]],"unit":"mm"},
            "rve":{  # ConcreteRVE
                "origin":{"value":[5,5,5],"unit":"mm"},
                "size":{ "value":[150,161,244],"unit":"um" },
                "ct":{ # CTScan
                    "id":"scan-000"
                },
                "materials":[
                    { # MaterialRecord
                        "name":"mat0",
                        "props":{"origin":"CZ","year":2018,"quality":"good"},
                    },
                    { # MaterialRecord
                        "name":"mat1",
                        "props":{"origin":"PL","year":2016,"project":"HTL-344PRP"},
                    }
                ]
            },
        }
    },
    "cs": ".beam.cs", # relative link to the ../beam/cs object
    "npointz": 2,
    "csState":[
        { # CrossSectionState
            "eps_axial": { "value":344, "unit":"um/m" },
            "bendingMoment": { "value":869, "unit":"kN*m" },
            "rveStates":[ 
                { # ConcreteRVEState
                    "rve":"...beam.cs.rve", # rel 
                    "sigmaHom": { "value": 89.5, "unit":"MPa" }
                },
                { # ConcreteRVEState
                    "rve":"...beam.cs.rve", # rel 
                    "sigmaHom": { "value": 81.4, "unit":"MPa" }
                },
            ]
        },
        { # CrossSectionState
            "eps_axial": { "value":878, "unit":"um/m" },
            "bendingMoment": { "value":123, "unit":"kN*m" },
            "rveStates":[ 
                { # ConcreteRVEState
                    "rve":"...beam.cs.rve", # rel 
                    "sigmaHom": { "value": 55.6, "unit":"MPa" }
                },
            ]
        },

    ],

}


class Test_POST_GET(unittest.TestCase):
    def test_01_post(self):
        C=self.__class__
        C.ID_01=post('dms0/BeamState',**dta)
    def test_02_get(self):
        C=self.__class__
        d=get(f'dms0/BeamState/{C.ID_01}',meta=True,tracking=False)
        # check that relative link was correctly interpreted
        self.assertEqual(d['cs']['_meta']['id'],d['beam']['cs']['_meta']['id'])
        # check that units were converted
        self.assertEqual(d['beam']['length']['unit'],'m')
        self.assertEqual(d['beam']['length']['value'],2.5)
        # check type metadata
        self.assertEqual(d['_meta']['type'],'BeamState')
        self.assertEqual(d['beam']['_meta']['type'],'Beam')
    def test_03_tracking(self):
        C=self.__class__
        d=get(f'dms0/BeamState/{C.ID_01}',meta=False,tracking=True)
        # relative link is recovered via object tracking
        self.assertEqual(d['cs'],'.beam.cs')
        self.assertEqual(d['csState'][0]['rveStates'][0]['rve'],'...beam.cs.rve')
        # metadata not returned
        self.assertTrue('_meta' not in d)
    def test_05_max_level(self):
        C=self.__class__
        d=get(f'dms0/BeamState/{C.ID_01}',meta=False,tracking=True,max_level=0)
        self.assertTrue('cs' not in d)
        self.assertTrue('npointz' in d)

    def test_99_float_error(self):
        beamDta={ # Beam 
            "length": { "value": 1, "unit":"km" },
            "height": { "value": 12.3456789, "unit":"cm" },
            "density": { "value": 3.456789, "unit":"g/cm3" },
        }
        ID=post('dms0/Beam',**beamDta)
        b=get(f'dms0/Beam/{ID}',meta=False,tracking=False)
        #
        self.assertEqual(b['height']['unit'],'m')
        self.assertGreater(b['height']['value'],0.1234567)


if __name__=='__main__':
    unittest.main()
