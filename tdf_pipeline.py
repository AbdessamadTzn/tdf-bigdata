import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import bigquery
import os
from dotenv import load_dotenv

load_dotenv()

PROJECT_NAME = os.getenv('PROJECT_NAME')
DATASET_LOCATION = os.getenv('DATASET_LOCATION')
CSV_TOREAD = os.getenv('CSV_TOREAD')


# Create thie bigquery dataset if it does not exist
def create_bigquery_dataset():
    """Create a BigQuery dataset if it does not exist."""
    client = bigquery.Client(project=PROJECT_NAME)
    dataset_id = f"{client.project}.tdf_data"
    
    try:
        # Check if the dataset exists
        client.get_dataset(dataset_id)
        print(f"Le dataset {dataset_id} existe déjà.")
    except Exception:
        # Create the dataset if it does not exist
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = DATASET_LOCATION 
        dataset = client.create_dataset(dataset)
        print(f"Dataset {dataset_id} créé avec succès.")

# Define the DoFn to process each line of the CSV
class CsvToBigQuery(beam.DoFn):
    def __init__(self):
        beam.DoFn.__init__(self)
    
    def process(self, element):
        import csv
        
        try:
            # Ignore null or empty elements
            if not element or element.isspace():
                return []
                
            row = next(csv.reader([element]))
            
            # Verify if the row has enough columns
            if len(row) < 12:
                print(f"Ignored lines (too short): {row}")
                return []
                
            # Convert values to appropriate types and handle missing values
            try:
                year = int(row[0]) if row[0].strip() else None
                tour_no = int(row[1]) if row[1].strip() else None
                winner = row[2].strip() if len(row) > 2 else ""
                country = row[3].strip() if len(row) > 3 else ""
                team = row[4].strip() if len(row) > 4 else ""
                tour_length = float(row[5]) if row[5].strip() else None
                age = int(row[6]) if row[6].strip() else None
                bmi = float(row[7]) if row[7].strip() else None
                weight_kg = float(row[8]) if row[8].strip() else None
                height_m = float(row[9]) if row[9].strip() else None
                rider_type = row[10].strip() if len(row) > 10 else ""
                close_rider_type = row[11].strip() if len(row) > 11 else ""
                
                return [{
                    'year': year,
                    'tour_no': tour_no,
                    'winner': winner,
                    'country': country,
                    'team': team,
                    'tour_overall_length_km': tour_length,
                    'age': age,
                    'bmi': bmi,
                    'weight_kg': weight_kg,
                    'height_m': height_m,
                    'rider_type': rider_type,
                    'close_rider_type': close_rider_type
                }]
            except (ValueError, IndexError) as e:
                print(f"Erreur lors du traitement de la ligne: {row}, erreur: {str(e)}")
                return []
        except Exception as e:
            print(f"Erreur lors du parsing CSV: {str(e)} pour l'élément: {element}")
            return []

def run():
    # Config Dataflow pipeline 
    pipeline_options = PipelineOptions(
        flags=[],
        runner='DataflowRunner',
        project=PROJECT_NAME,
        temp_location='gs://tdf-bucket/temp/',
        region=DATASET_LOCATION
    )

    # Bigquery schema
    schema = {
        'fields': [
            {'name': 'year', 'type': 'INTEGER'},
            {'name': 'tour_no', 'type': 'INTEGER'},
            {'name': 'winner', 'type': 'STRING'},
            {'name': 'country', 'type': 'STRING'},
            {'name': 'team', 'type': 'STRING'},
            {'name': 'tour_overall_length_km', 'type': 'FLOAT'},
            {'name': 'age', 'type': 'INTEGER'},
            {'name': 'bmi', 'type': 'FLOAT'},
            {'name': 'weight_kg', 'type': 'FLOAT'},
            {'name': 'height_m', 'type': 'FLOAT'},
            {'name': 'rider_type', 'type': 'STRING'},
            {'name': 'close_rider_type', 'type': 'STRING'}
        ]
    }

    # Create the pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        (p
         | 'Read CSV' >> beam.io.ReadFromText(CSV_TOREAD, skip_header_lines=1)
         | 'Transform lines' >> beam.ParDo(CsvToBigQuery())
         | 'Send to BigQuery' >> beam.io.WriteToBigQuery(
             'tourdefrance-bigdata:tdf_data.winners',
             schema=schema,
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
        )

if __name__ == '__main__':
    # Create dataset in BigQuery if it does not exist
    create_bigquery_dataset()
    # Execute pipeline
    run()