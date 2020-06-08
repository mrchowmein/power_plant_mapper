import unittest
from mapping import *
from pandas._testing import assert_frame_equal

class MappingTestCase(unittest.TestCase):
    """Test for 'mapping.py"""

    def test_clean_entso(self):
        df_correct = pd.DataFrame({'unit_id':[123,456,9865],'unit_capacity_mw':[10,25,200],'unit_fuel':['fossil hard coal',
                                'fossil gas', 'nuclear'],'country':['FRANCE','GERMANY','UNITED KINGDOM'],
                                   'unit_name':['NICE 2', 'FRANKFURT 6', 'LONDON FUSION 1'],'plant_name':['NICE ENERGY','DE ENERGY','LONDON FUSION REACTOR'],
                                   'plant_capacity_mw':[45, 90, 390], 'country_platts': ['FRANCE', 'GERMANY', 'ENGLAND & WALES']})

        df = clean_entso(pd.read_csv("test_entso.csv"))

        assert_frame_equal(df, df_correct)



    def test_clean_platts(self):
        df_p = pd.read_csv("test_platts.csv", header=0)
        df_p = clean_platts(df_p)

        self.assertEqual(df_p.iloc[0, 3], 'FRANKFURT B6')
        self.assertEqual(df_p.iloc[1, 2], 'LONDON FUSION REACTOR')

    def test_clean_gppd(self):
        df_g = pd.read_csv("test_gppd.csv", header=0)
        df_g = clean_gppd(df_g)

        print(df_g.iloc[1, 1])
        self.assertEqual(df_g.iloc[0, 8], 'NUCLEAR')
        self.assertEqual(df_g.iloc[1, 1], 'LONDON FUSION REACTOR')

if __name__ == '__main__':
    unittest.main()
