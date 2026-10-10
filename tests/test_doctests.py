import doctest
from intelligent_futures import policies


def load_tests(loader, tests, ignore):
    tests.addTests(doctest.DocTestSuite(policies))
    return tests
