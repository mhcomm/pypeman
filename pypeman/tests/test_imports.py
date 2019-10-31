from pypeman.test import TearDownProjectTestCase as TestCase


class ImportTests(TestCase):
    def test_ftp_import(self):
        import pypeman.contrib.ftp  # noqa: F401

    def test_hl7_import(self):
        import pypeman.contrib.hl7  # noqa: F401

    def test_http_import(self):
        import pypeman.contrib.http  # noqa: F401

    def test_time_import(self):
        import pypeman.contrib.time  # noqa: F401

    def test_xml_import(self):
        import pypeman.contrib.xml  # noqa: F401
