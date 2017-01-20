import apache_beam as beam
from apache_beam.io import iobase
from apache_beam.io.range_trackers import OffsetRangeTracker
from apache_beam.transforms.core import PTransform
from apache_beam.utils.pipeline_options import PipelineOptions

class CountingSource(iobase.BoundedSource):

    def __init__(self, count):
        self._count = count

    def estimate_size(self):
        return self._count

    def get_range_tracker(self, start_position, stop_position):
        if start_position is None:
            start_position = 0
        if stop_position is None:
            stop_position = self._count

        return OffsetRangeTracker(start_position, stop_position)

    def read(self, range_tracker):
        for i in range(self._count):
            if not range_tracker.try_claim(i):
                return
            yield i

    def split(self, desired_bundle_size, start_position=None,
              stop_position=None):
        if start_position is None:
            start_position = 0
        if stop_position is None:
            stop_position = self._count

        bundle_start = start_position
        while bundle_start < self._count:
            bundle_stop = max(self._count, bundle_start + desired_bundle_size)
            yield iobase.SourceBundle(weight=(bundle_stop - bundle_start),
                                      source=self,
                                      start_position=bundle_start,
                                      stop_position=bundle_stop)
            bundle_start = bundle_stop