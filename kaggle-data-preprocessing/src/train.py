#-*- coding: utf-8 -*-
import apache_beam as beam
import csv
import json
import os
import re
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json


#cloud
# class preprocessing(beam.DoFn):
#     def __init__(self, default=''):
#             self.default = default
#
#     def process(self, fields):
#             fields = fields.split(",")
#             #json_data = json.loads(fields)
#             header = "label"
#             for i in range(0, 784):
#                 header += (",pixel" + str(i))
#
#             label_list_str = "["
#             label_list = []
#             for i in range(0,10) :
#                 if fields[0] == str(i) :
#                     #label_list.append(i)
#                     label_list_str+=str(i)
#                 else :
#                     #label_list.append(0)
#                     label_list_str+=("0")
#                 if i!=9 :
#                     label_list_str+=","
#             label_list_str+="],"
#             for i in range(1,len(fields)) :
#                 label_list_str+=fields[i]
#                 if i!=len(fields)-1:
#                     label_list_str+=","
#
#             yield label_list_str
#
#     def finish_bundle(self):
#
#         yield beam.utils.windowed_value.WindowedValue(
#             value=dict,
#             timestamp=0,
#             windows=[self.window],
#         )
def test(sets) :
    return sets
def side_input_header(suffix, prefix) :
    length = 0
    for idx, i in enumerate(prefix) :
        length += 1
    header = ""
    for idx, i in enumerate(prefix) :
        if idx == length-1 :
            header+=i
        else :
            header+=(i+',')
    yield header+'\n'+suffix

class testFn(beam.CombineFn):
    def __init__(self,default=','):
        self.default = default

    def process(self, fields):
        fields = fields.split(",")
        #json_data = json.loads(fields)
        header = "label"
        for i in range(0, 784):
            header += (",pixel" + str(i))

        label_list_str = "["
        label_list = []
        for i in range(0,10) :
            if fields[0] == str(i) :
                #label_list.append(i)
                label_list_str+=str(i)
            else :
                #label_list.append(0)
                label_list_str+=("0")
            if i!=9 :
                label_list_str+=","
        label_list_str+="],"
        for i in range(1,len(fields)) :
            label_list_str+=fields[i]
            if i!=len(fields)-1:
                label_list_str+=","

        yield label_list_str


def run(project, bucket, dataset) :
        argv = [
            "--project={0}".format(project),
            "--job_name=kaggle-dataflow",
            "--save_main_session",
            "--region=asia-northeast1",
            "--staging_location=gs://{0}/kaggle-bucket-v1/".format(bucket),
            "--temp_location=gs://{0}/kaggle-bucket-v1/".format(bucket),
            "--max_num_workers=8",
            "--worker_region=asia-northeast3",
            "--worker_disk_type=compute.googleapis.com/projects//zones//diskTypes/pd-ssd",
            "--autoscaling_algorithm=THROUGHPUT_BASED",
            #"--runner=DataflowRunner",
            "--worker_region=asia-northeast3"
            "--runner=DirectRunner"
        ]

        #events_output = "{}:{}.TB_PAGE_STAT".format(project, dataset)
        result_output = 'gs://kaggle-bucket-v1/result/result.csv'
        filename = "gs://{}/train.csv".format(bucket)
        pipeline = beam.Pipeline(argv=argv)
        valid_durations = pipeline | 'Valid durations' >> beam.Create(
            ['column1', 'column2', 'column3']
        )
        ptransform = (pipeline
                      | "Read from GCS" >> beam.io.ReadFromText(filename)
                      | "Kaggle data pre-processing" >> beam.CombineGlobally(testFn(','))
                      #| "Make Table Row" >> beam.ParDo(makeBqTable(","))
                      #| "Print" >> beam.Map(print)
                      )
        # schema = "_id:record,_id.oid:string,totalid:string,article_title:string,urlpath:string,service_date:record,service_date.date:string,section:string,create_date:record,create_date.date:timestamp,article_flag:string,source_code:integer,"\
        #         "source_name:string,press_date:record,press_date.date:string"

        #data_ingestion = DataTransformation()
        #schema = parse_table_schema_from_json(data_ingestion.schema_str)



        (ptransform
            # | 'combine processing' >> beam.CombineGlobally(lambda sets, single :\
            #                                                single+'\n'+sets,
            #                                                single=beam.pvalue.AsSingleton(valid_durations)
            #                                                )
            | 'side-input' >> beam.FlatMap(
                    side_input_header,
                    beam.pvalue.AsIter(valid_durations)
                )

         | beam.Map(print))
        #   | "Write_to_GCS" >> beam.io.WriteToText(
        # "gs://{0}/temp/".format(bucket), file_name_suffix=".json")
        # | "events:out" >> beam.io.WriteToText(
        #         result_output, num_shards=1
        #     )
        #  )
        #)

        pipeline.run()

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run pipeline on the cloud")
    parser.add_argument("--project", dest="project", help="Unique project ID", required=True)
    parser.add_argument("--bucket", dest="bucket", help="Bucket where your data were ingested", required=True)
    parser.add_argument("--dataset", dest="dataset", help="BigQuery dataset")
    #parser.add_argument("--labels", help="labels")
    #parser.add_argument("--job_name", help="job_name")
    #parser.add_argument("--region", help="region", required=True)
    #parser.add_argument("--runner", help="runner")

    args = vars(parser.parse_args())

    print("Correcting timestamps and writing to BigQuery dataset {}".format(args["dataset"]))
    run(project=args["project"], bucket=args["bucket"], dataset=args["dataset"])
