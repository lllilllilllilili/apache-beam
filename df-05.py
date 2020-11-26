#!/usr/bin/env python3

# Copyright 2016 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import apache_beam as beam
import csv

DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S'


def addtimezone(lat, lon):
    try:
        import timezonefinder
        tf = timezonefinder.TimezoneFinder()
        return (lat, lon, tf.timezone_at(lng=float(lon), lat=float(lat)))
        # return (lat, lon, 'America/Los_Angeles') # FIXME
    except ValueError:
        return (lat, lon, 'TIMEZONE')  # header


def as_utc(date, hhmm, tzone):
    try:
        if len(hhmm) > 0 and tzone is not None:
            import datetime, pytz
            loc_tz = pytz.timezone(tzone)
            loc_dt = loc_tz.localize(datetime.datetime.strptime(date, '%Y-%m-%d'), is_dst=False)
            # can't just parse hhmm because the data contains 2400 and the like ...
            loc_dt += datetime.timedelta(hours=int(hhmm[:2]), minutes=int(hhmm[2:]))
            utc_dt = loc_dt.astimezone(pytz.utc)
            return utc_dt.strftime(DATETIME_FORMAT), loc_dt.utcoffset().total_seconds()
        #utcoffset() returns the UTC Offset of the datetime instance.
        #UTC+09:00에서 +09:00의 의미는 UTC의 기준시간 보다 9시간이 빠르다는 의미이다.
        #즉 UTC 기준으로 현재 낮 12시 라면 한국 시간으로 오후 9시(21시)가 될것이다.
        #이렇게 UTC와의 차이를 나타낸 것을 오프셋이라고 하며, +09:00 혹은 -03:00등과 같이 표현된다.


        else:
            return '', 0  # empty string corresponds to canceled flights
    except ValueError as e:
        print('{} {} {}'.format(date, hhmm, tzone))
        raise e


def add_24h_if_before(arrtime, deptime):
    import datetime
    if len(arrtime) > 0 and len(deptime) > 0 and (arrtime < deptime):
        #도착시간이 출발시간보다 작을 수 있나?
        adt = datetime.datetime.strptime(arrtime, DATETIME_FORMAT)
        adt += datetime.timedelta(hours=24)
        return adt.strftime(DATETIME_FORMAT)
    else:
        return arrtime


def tz_correct(line, airport_timezones):
    fields = line.split(',')

    #line 전달받은 데이터 값, airport_timezones->AsDict
    if fields[0] != 'FL_DATE' and len(fields) == 27:
        # convert all times to UTC
        dep_airport_id = fields[6]
        arr_airport_id = fields[10]
        dep_timezone = airport_timezones[dep_airport_id][2]
        arr_timezone = airport_timezones[arr_airport_id][2]

        for f in [13, 14, 17]:  # crsdeptime, deptime, wheelsoff
            fields[f], deptz = as_utc(fields[0], fields[f], dep_timezone)
        for f in [18, 20, 21]:  # wheelson, crsarrtime, arrtime
            fields[f], arrtz = as_utc(fields[0], fields[f], arr_timezone)

        for f in [17, 18, 20, 21]:
            fields[f] = add_24h_if_before(fields[f], fields[14])

        fields.extend(airport_timezones[dep_airport_id]) #?

        fields[-1] = str(deptz) #?
        fields.extend(airport_timezones[arr_airport_id]) #?
        fields[-1] = str(arrtz) #?


        yield fields


def get_next_event(fields):
    #event 생성, 이벤트를 출발 및 도착 메시지로만 제한한다.

    if len(fields[14]) > 0:
        event = list(fields)  # copy
        event.extend(['departed', fields[14]])
        for f in [16, 17, 18, 19, 21, 22, 25]:
            event[f] = ''  # not knowable at departure time, 출발 시간 알 수 없음
        print(event)
        yield event
    if len(fields[21]) > 0:
        event = list(fields)
        event.extend(['arrived', fields[21]])
        print(event)
        yield event



def run():
    with beam.Pipeline('DirectRunner') as pipeline:
        airports = (pipeline
                    | 'airports:read' >> beam.io.ReadFromText('airports.csv.gz')
                    | 'airports:fields' >> beam.Map(lambda line: next(csv.reader([line])))
                    | 'airports:tz' >> beam.Map(lambda fields: (fields[0], addtimezone(fields[21], fields[26])))
                    )

        flights = (pipeline
                   | 'flights:read' >> beam.io.ReadFromText('201501_part.csv')
                   | 'flights:tzcorr' >> beam.FlatMap(tz_correct, beam.pvalue.AsDict(airports))
                   )
        # 항공편 데이터에서 출발 공항 ID를 얻어 공항 데이터에서 공항 ID에 대한 시간대를 찾는다.
        # 유사하게 도착 이벤트를 생성하기 위해 도착시간을 사용한다. 파이프라인에서는 현지 시간을 UTC로 변환한 후 항공편 PCollection에서 호출한다.

        (flights
         | 'flights:tostring' >> beam.Map(lambda fields: ','.join(fields))
         | 'flights:out' >> beam.io.textio.WriteToText('all_flights')
         )

        events = flights | beam.FlatMap(get_next_event)

        (events
         | 'events:tostring' >> beam.Map(lambda fields: ','.join(fields))
         | 'events:out' >> beam.io.textio.WriteToText('all_events')
         )

        pipeline.run()


if __name__ == '__main__':
    run()
