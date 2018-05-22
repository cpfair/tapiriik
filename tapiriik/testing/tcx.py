from tapiriik.testing.testtools import TestTools, TapiriikTestCase
from tapiriik.services.tcx import TCXIO

import os

class TCXTests(TapiriikTestCase):
    def test_constant_representation(self):
        ''' ensures that tcx import/export is symetric '''
        script_dir = os.path.dirname(__file__)
        rel_path = "data/test1.tcx"
        source_file_path = os.path.join(script_dir, rel_path)
        with open(source_file_path, 'r') as testfile:
            data = testfile.read()

        act = TCXIO.Parse(data.encode('utf-8'))
        new_data = TCXIO.Dump(act)
        act2 = TCXIO.Parse(new_data.encode('utf-8'))
        rel_path = "data/output1.tcx"
        new_file_path = os.path.join(script_dir, rel_path)
        with open(new_file_path, "w") as new_file:
            new_file.write(new_data)

        self.assertActivitiesEqual(act2, act)