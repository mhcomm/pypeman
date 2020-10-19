from pypeman.test import PypeTestCase
# from pypeman.message import Message


class MyChanTest(PypeTestCase):

    def test1_great_channel(self):
        """ Test example """
        self.get_channel("periodic")
