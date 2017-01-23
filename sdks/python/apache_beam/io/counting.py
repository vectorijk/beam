#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from apache_beam.io import iobase
from apache_beam.io.range_trackers import OffsetRangeTracker
from apache_beam.pvalue import PBegin
from apache_beam.transforms.ptransform import PTransform
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


class _BoundedCountingInput(PTransform):
    def __init__(self, *args):
        if len(*args) == 1:
            self.startIndex = 0
            self.endIndex = args[0]
        elif len(*args) == 2:
            self.startIndex = args[0]
            self.endIndex = args[1]

    def expand(self, pbegin):
        assert isinstance(pbegin, PBegin)
        self.pipeline = pbegin.pipeline
        pbegin.apply(iobase.Read(CountingSource.get_range_tracker(start_position=self.startIndex,
                                                                  stop_position=self.endIndex)))
        return pvalue.PCollection(self.pipeline)

class CountingInput:
    def __init__(self):
        pass

    def expand(self):
        pass

    def up_to(self, numElements):