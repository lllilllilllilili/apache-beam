import apache_beam as beam

def normalize_and_validate_durations(plant, valid_durations):
  # plant['duration'] = plant['duration'].lower()
  # if plant['duration'] in valid_durations:
  length = 0
  for idx, i in enumerate(valid_durations) :
      length+=1

  header = ""
  for idx, i in enumerate(valid_durations) :
      if idx == length-1 :
          header+=i
      else :
          header+=(i+',')
  yield header+'\n'+plant

with beam.Pipeline() as pipeline:
  valid_durations = pipeline | 'Valid durations' >> beam.Create(
      ['column1', 'column2', 'column3']
  )

  valid_plants = (
      pipeline
      | 'Gardening plants' >> beam.Create([
          'test test test'
      ])
      | 'Normalize and validate durations' >> beam.FlatMap(
          normalize_and_validate_durations,
          valid_durations=beam.pvalue.AsIter(valid_durations),
      )
      | beam.Map(print))