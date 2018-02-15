import unittest

class ImportTests(unittest.TestCase):
    def test_ftp_import(self):
        import pypeman.contrib.ftp
    def test_hl7_import(self):
        import pypeman.contrib.hl7
    def test_http_import(self):
        import pypeman.contrib.http
    def test_time_import(self):
        import pypeman.contrib.time
    def test_xml_import(self):
        import pypeman.contrib.xml

