import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
import unittest
import virginmedia_pipeline

class TestDate_And_Amount(unittest.TestCase):

  def test_date_and_amount(self):
    dates = (
      ["2009-01-09 02:54:25 UTC", "a", "b", "1000"],
      ["2017-01-01 04:22:23 UTC","c",	"d", "25"],
      ["2022-10-11 08:22:11 UTC" , "e", "f", "121122"],
      ["2022-11-12 10:10:10 UTC", "g" , "h", "98980" ],
      ["2010-01-02 10:22:11 UTC", "i", "j", "7878"],
      ["2020-01-22 10:32:33 UTC", "k", "l", "19"]
    )

    with TestPipeline() as p:

      input = p | beam.Create(dates)

      output = input | virginmedia_pipeline.CompositeTransform()

      assert_that(
        output,
        equal_to(
          [{"2017-01-01": 25}, {"2022-01-01": 220102}, {"2010-01-01":7878}]
        )
      )

if __name__ == '__main__':
    unittest.main()