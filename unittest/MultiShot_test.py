import unittest
import MultiShot
import utils as aide
import logging

class Test_MultiShot(unittest.TestCase):
    # one_shot_iterator
    def test_oneshot_iterator(self):
        # TODO
        self.assertEqual(True, True)

    def test_match_oneshot(self):
        '''
        matches oneshot files and queries together
        '''
        fmt = '%(asctime)s  %(levelname)-9s  %(message)s'
        logfile = 'MultiShot.log'
        logging.basicConfig(level=logging.DEBUG, format=fmt, filename=logfile)

        queries = ['test.rq', 'test2.rq', 'te_st.rq']
        oneshots = ['clean_test.py', 'clean_te_st.py']

        pairs = MultiShot.match_oneshot_to_query(oneshots, queries, logging)

        expected = {'test.rq': 'clean_test.py', 'te_st.rq': 'clean_te_st.py'}
        self.assertEqual(expected, pairs)

    def test_load_oneshots(self):
        '''
        tests file managment
        '''
        fmt = '%(asctime)s  %(levelname)-9s  %(message)s'
        logfile = 'MultiShot.log'
        logging.basicConfig(level=logging.DEBUG, format=fmt, filename=logfile)

        queries_path = "unittest/test_data/test_queries"
        oneShot_path = "unittest/test_data/test_oneShots"
        all_oneshots, all_queries = MultiShot.load_oneshots(queries_path, oneShot_path, logging)

        expected_oneshots = ['clean_unittest.py']
        expected_queries = ['unittest.rq']

        self.assertEqual(expected_oneshots, all_oneshots)
        self.assertEqual(expected_queries, all_queries)

    def test_check_type(self):
        clean_name1 = 'unittest'
        type1 = MultiShot.check_Type(clean_name1)
        expected1 = 'misc_audits'

        clean_name2 = 'trying to find pubs_'
        type2 = MultiShot.check_Type(clean_name2)
        expected2 = 'pub_audits'

        clean_name3 = 'trying to find ___misc_'
        type3 = MultiShot.check_Type(clean_name3)
        expected3 = 'misc_audits'

        self.assertEqual(expected1, type1)
        self.assertEqual(expected2, type2)
        self.assertEqual(expected3, type3)

