import unittest
import requests


class TestObjectsMethods(unittest.TestCase):
    first_object="ZTF18aaccpyz"
    params = {
        "oid": first_object
    }

    def test_detections(self):
        resp = requests.post("http://localhost:8085/get_detections",json=self.params)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(type(resp.json()),dict)

    def test_non_detections(self):
        resp = requests.post("http://localhost:8085/get_non_detections",json=self.params)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(type(resp.json()),dict)

    def test_features(self):
        resp = requests.post("http://localhost:8085/get_features",json=self.params)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(type(resp.json()),dict)

    def test_stats(self):
        resp = requests.post("http://localhost:8085/get_stats",json=self.params)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(type(resp.json()),dict)

    def test_probabilities(self):
        resp = requests.post("http://localhost:8085/get_probabilities",json=self.params)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(type(resp.json()),dict)

    def test_period(self):
        resp = requests.post("http://localhost:8085/get_period",json=self.params)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(type(resp.json()),dict)

    def test_magref(self):
        resp = requests.post("http://localhost:8085/get_magref",json=self.params)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(type(resp.json()),dict)

if __name__ == '__main__':
    unittest.main()
