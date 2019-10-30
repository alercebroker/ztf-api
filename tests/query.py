import unittest
import requests
import pandas as pd

class TestObjectsMethods(unittest.TestCase):
    def test_oid(self):
        first_object="ZTF18aaccpyz"
        params = {
            "query_parameters":{
                "filters":{
                    "oid": first_object
                }
            }
        }
        resp = requests.post("http://localhost:8085/query",json=params)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(type(resp.json()),dict)
        self.assertEqual(resp.json()["total"], 1)


    def test_nobs(self):
        nobs_min = 10
        nobs_max = 15
        params = {
            "query_parameters":{
                "filters":{
                    "nobs": {
                        "min": nobs_min,
                        "max": nobs_max
                    }
                }
            }
        }
        resp = requests.post("http://localhost:8085/query",json=params)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(type(resp.json()),dict)
        df = pd.DataFrame(resp.json()["result"])
        nobs = df.loc["nobs"]
        self.assertLessEqual(nobs.min(),nobs_max)
        self.assertGreaterEqual(nobs.max(),nobs_min)


    def test_magap(self):
        magap_min = 15
        magap_max = 18
        params = {
            "query_parameters":{
                "filters":{
                    "median_magap_r": {
                        "min": magap_min,
                        "max": magap_max
                    },
                    "median_magap_g": {
                        "min": magap_min,
                        "max": magap_max
                    }
                }
            }
        }
        resp = requests.post("http://localhost:8085/query",json=params)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(type(resp.json()),dict)
        df = pd.DataFrame(resp.json()["result"])
        magap = df.loc["median_magap_g"]
        self.assertLessEqual(magap.min(),magap_max)
        self.assertGreaterEqual(magap.max(),magap_min)

    def test_coordinates(self):
        params = {
            "query_parameters":{
                "coordinates":{
                    "ra": 0,
                    "dec": 0,
                    "sr": 100
                }
            }
        }
        resp = requests.post("http://localhost:8085/query",json=params)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(type(resp.json()),dict)

    def test_dates(self):
        min_mjd = 58271.1836342998
        max_mjd = 58275.1676619998
        params = {
            "query_parameters":{
                "dates":{
                    "firstmjd":{
                        "min":min_mjd,
                        "max":max_mjd
                    }
                }
            }
        }
        resp = requests.post("http://localhost:8085/query",json=params)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(type(resp.json()),dict)
        df = pd.DataFrame(resp.json()["result"])
        firstmjd = df.loc["firstmjd"]
        self.assertLessEqual(firstmjd.min(),max_mjd)
        self.assertGreaterEqual(firstmjd.max(),min_mjd)


if __name__ == '__main__':
    unittest.main()
