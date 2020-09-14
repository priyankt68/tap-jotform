#!/usr/bin/env python3
import os
import argparse
import json
import collections
import requests
import singer
from singer import utils, metadata
import singer.bookmarks as bookmarks
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema
import singer.metrics as metrics

REQUIRED_CONFIG_KEYS = ["api_key", "start_date", "is_hipaa_safe_mode_on"]
PER_PAGE = 100

SESSION = requests.Session()
LOGGER = singer.get_logger()

KEY_PROPERTIES = {
  'submission': ['id']
}

# need to move inside sync
HEADERS = {'APIKEY': config['api_key']}
URL = "https://hipaa-api.jotform.com/"

# need to be converted to clients
ENDPOINTS = {
  "forms": "user/forms",
  "submissions": "/form/{form_id}/submissions"
}


class AuthException(Exception):
  pass

class NotFoundException(Exception):
  pass

def translate_state(state, catalog, form_id):
  """
  state looks like:
    {
      "bookmarks": {
        "submissions": {
          "id": 1234412
        }
      }
    }
  """
  pass

def get_bookmark(state, form_id, stream_name, bookmark_key):
  pass


def authed_get(source, url, headers=None, query_params=None):
  headers = headers if headers else {}
  query_params = query_params if query_params else {}
  with metrics.http_request_timer(source) as timer:
    SESSION.headers.update(headers)
    resp = SESSION.get(url=url, params=query_params)
    resp.raise_for_status()

    timer.tags[metrics.Tag.http_status_code] = resp.status_code
    return resp

def authed_get_all_pages(source, url, headers, query_params):
  offset, limit = 0, 20
  while True:
    resp = authed_get(source, url, headers, query_params)
    resp.raise_for_status()
    yield resp
    query_params['offset'] = query_params['offset'] + limit
    query_params['limit'] = limit



def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    """ Load schemas from schemas folder """
    schemas = {}

    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = json.load(file)

    return schemas



def get_all_submissions(schema, form_id, state, mdata):
  '''
  https://hipaa-api.jotform.com/form/{form_id}/submissions
  '''
  query_params = {}
  bookmark = get_bookmark(state, form_id, "submissions", "id")
  if bookmark:
    query_params["id:gt"] = bookmark

  with metrics.record_counter('submissions') as counter:
    for response in authed_get_all_pages(
      'submissions',
      f'https://hipaa-api.jotform.com/form/{form_id}/submissions',
      query_params=query_params,
      headers=HEADERS
    ):
      submissions = response.json()
      extraction_time = singer.utils.now()
      for submission in submissions.get('content'):
        # Transform the event
        with singer.Transformer() as transformer:
                    record = transformer.transform(submission, schema, metadata=metadata.to_map(mdata))
        singer.write_record('submissions', record, time_extracted=extraction_time )
        singer.write_bookmark(state, form_id, 'submissions', {'id': submissions['id']}
        counter.increment()
  return state

def discover():
    raw_schemas = load_schemas()
    streams = []
    for stream_id, schema in raw_schemas.items():
        # TODO: populate any metadata and stream's key properties here..
        stream_metadata = []
        key_properties = []
        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=key_properties,
                metadata=stream_metadata,
                replication_key=None,
                is_view=None,
                database=None,
                table=None,
                row_count=None,
                stream_alias=None,
                replication_method=None,
            )
        )
    return Catalog(streams)


def sync(config, state, catalog):
    """ Sync data from tap source """
    # Loop over selected streams in catalog
    import pdb; pdb.set_trace()
    # TODO: figure out how to make selected work
    for stream in catalog.streams:
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)

        bookmark_column = stream.replication_key
        is_sorted = True  # TODO: indicate whether data is sorted ascending on bookmark value

        singer.write_schema(
            stream_name=stream.tap_stream_id,
            schema=stream.schema.to_dict(),
            key_properties=stream.key_properties,
        )

        url = f"https://hipaa-api.jotform.com/form/{config['form_id']}/submissions"
        headers = {'APIKEY': config['api_key']}
        res = requests.get(url, headers=headers)

        # TODO: delete and replace this inline function with your own data retrieval process:
        tap_data = lambda: [{"id": x, "name": "row${x}"} for x in range(1000)]


        max_bookmark = None
        for row in res.json()["content"]:
            # TODO: place type conversions or transformations here
            import pdb; pdb.set_trace()
            # write one or more rows to the stream:
            singer.write_records(stream.tap_stream_id, [row])
            if bookmark_column:
                if is_sorted:
                    # update bookmark to latest value
                    singer.write_state({stream.tap_stream_id: row[bookmark_column]})
                else:
                    # if data unsorted, save max value until end of writes
                    max_bookmark = max(max_bookmark, row[bookmark_column])
        if bookmark_column and not is_sorted:
            singer.write_state({stream.tap_stream_id: max_bookmark})
    return


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        catalog.dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
        sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()
