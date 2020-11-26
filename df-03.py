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

def addtimezone(lat, lon):
   try:
      import timezonefinder
      tf = timezonefinder.TimezoneFinder()
      return (lat, lon, tf.timezone_at(lng=float(lon), lat=float(lat)))
      #return (lat, lon, 'America/Los_Angeles') # FIXME
   except ValueError:
      return (lat, lon, 'TIMEZONE') # header

def as_utc(date, hhmm, tzone):
   # 각 공항의 시간대로 보고된 날짜 및 시간을 UTC로 변환됩니다.
   try:
      if len(hhmm) > 0 and tzone is not None:
         import datetime, pytz
         loc_tz = pytz.timezone(tzone)
         loc_dt = loc_tz.localize(datetime.datetime.strptime(date,'%Y-%m-%d'), is_dst=False)
         # strptime : method를 사용하여 문자열로부터 datetime 오브젝트를 만드는 방법
         # can't just parse hhmm because the data contains 2400 and the like ...
         print("loc_tz")
         print(loc_tz)
         print("loc_dt")
         print(loc_dt)

         loc_dt += datetime.timedelta(hours=int(hhmm[:2]), minutes=int(hhmm[2:]))
         utc_dt = loc_dt.astimezone(pytz.utc)
         print("loc_dt")
         print(loc_dt)
         print("utc_dt")
         print(utc_dt)
         #timedelta : 특정 시각에 얼마만큼의 시간을 더하거나 빼는 역할
         return utc_dt.strftime('%Y-%m-%d %H:%M:%S')
         # 시간을 UTC로 바꾼다.
      else:
         return ''
         # empty string corresponds to canceled flights
   except ValueError as e:
      print('{} {} {}'.format(date, hhmm, tzone))
      raise e

def tz_correct(line, airport_timezones):
   # 항공편 데이터에서 출발 공항 ID를 얻어 공항 데이터에서 공항 ID에 대한 시간대를 찾는다.
   fields = line.split(',')

   if fields[0] != 'FL_DATE' and len(fields) == 27: #첫 head 빼고
      # convert all times to UTC
      dep_airport_id = fields[6] #ORIGIN_AIRPORT_SEQ_ID
      arr_airport_id = fields[10] #DEST_AIRPORT_SEQ_ID
      dep_timezone = airport_timezones[dep_airport_id][2]
      arr_timezone = airport_timezones[arr_airport_id][2]
      # '1670901': ('29.34250000', '-98.85111111', 'America/Chicago')
      # '1671001': ('38.59694444', '-77.07250000', 'America/New_York')

      for f in [13, 14, 17]: #crsdeptime, deptime, wheelsoff
         fields[f] = as_utc(fields[0], fields[f], dep_timezone)
         #FL_DATE, 13=CRS_DEP_TIME, 14=DEP_TIME, WHEELS_OFF
          #WHEELS_OFF :항공기 바퀴가 지면을 벗어나는 시점
          #CRS_DEP_TIME :CRSDepTime- 현지 시간의 CRS 출발 시간 (hhmm), CRS : 컴퓨터를 통해 항공편의 예약



      for f in [18, 20, 21]: #wheelson, crsarrtime, arrtime
         fields[f] = as_utc(fields[0], fields[f], arr_timezone)
         #wheelson :
      yield ','.join(fields)


if __name__ == '__main__':
   with beam.Pipeline('DirectRunner') as pipeline:

      airports = (pipeline
         | 'airports:read' >> beam.io.ReadFromText('airports.csv.gz')
         | 'airports:fields' >> beam.Map(lambda line: next(csv.reader([line])))
         # next() 메소드로 데이터를 순차적으로 호출 가능, 마지막 데이터 이후 next()를 호출하면 Stopiteration 에러 발생
         # for문을 사용할 때, 파이썬 내부에서는 임시로 list를 iterator로 변환해서 사용

         | 'airports:tz' >> beam.Map(lambda fields: (fields[0], addtimezone(fields[21], fields[26])))
      )

      flights = (pipeline
         | 'flights:read' >> beam.io.ReadFromText('201501_part.csv')
         | 'flights:tzcorr' >> beam.FlatMap(tz_correct, beam.pvalue.AsDict(airports))
         # | beam.Map(print)
         # tz_correct 매개 인자 없이 넣어도 위에 flow 에서 처리된 데이터를 넣게 된다.
         # ('1', '2') 로 하면 tz_correct(first, second) 로 넣을 수 있음
       )
      #AIRPORT_SEQ_ID, LATITUDE, LONGITUDE
      # flights | beam.io.textio.WriteToText('all_flights')

      #pipeline.run()
