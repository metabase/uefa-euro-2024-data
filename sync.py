import dlt
import logging
from logic import full_load, load_season
from argparse import ArgumentParser
from config import DATASET_NAME, PIPELINE_NAME

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO)

pipeline = dlt.pipeline(
    pipeline_name=PIPELINE_NAME,
    destination="postgres",
    dataset_name=DATASET_NAME
)


if __name__ == '__main__':
    # Args
    parser = ArgumentParser()
    parser.add_argument('--full', action='store_true')
    args = parser.parse_args()

    # Dry run to initialize the pipeline
    pipeline.run()

    if args.full:
        full_load(pipeline)
    else:
        load_season(pipeline)
