import argparse
from termcolor import colored
import requests
import json
from pathlib import Path
import csv
from operator import itemgetter
import time


def main():
    # Create arg parser to load input vars
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--apiKey',
                            action='store',
                            type=str,
                            help='the api key for your account')
    arg_parser.add_argument('--accountId',
                            action='store',
                            type=str,
                            help='your account id for new relic')
    arg_parser.add_argument('--inputFile',
                            action='store',
                            type=str,
                            help='the location of the input files')
    arg_parser.add_argument('--jsonFile',
                            action='store',
                            type=str,
                            help='the location of the json files')
    arg_parser.add_argument('--outputFile',
                            action='store',
                            type=str,
                            help='the location of the output files')

    # Load arg parse vars into their own variables for ease of use
    args = arg_parser.parse_args()
    api_key = args.apiKey
    account_id = args.accountId
    input_file_path = args.inputFile
    tmp_file_path = args.jsonFile
    output_file_path = args.outputFile

    print("\nStarting New Relic Crawler"
          "\n\tAPI KEY: " + str(api_key) +
          "\n\tACCOUNT KEY: " + str(account_id) +
          "\n\tINPUT: " + input_file_path + "\n")

    # Start timer
    start = time.time()

    # Load input files
    input_txt_files = load_txt_files(input_file_path)

    # Fetch data from new relic
    for input_text_file in input_txt_files:

        json_file_contents = []
        print(colored("\n\nFetching query data for file: " + str(input_text_file), "yellow"))
        input_text_file_contents = input_txt_files.get(input_text_file)

        for line in input_text_file_contents:

            # Split | delimited line
            line_sections = line.split("|")
            header = line_sections[0]
            sub_header = line_sections[1]
            query_name = line_sections[2]
            query = line_sections[3]
            time_clause = line_sections[4]

            print("Fetching data for..." +
                  "\n\tQuery Name: " + str(colored(query_name, "blue")) +
                  "\n\tSQL: " + str(colored(query, "magenta")) +
                  "\n\tTime " + str(colored(time_clause, "yellow")))

            # Execute rest call to new relic
            response = execute_rest_call(query, api_key, account_id, time_clause)

            # Parse response from new relic
            parsed_response = parse_response(response)

            # Create json data holder for temp file
            tmp_json_container = {
                "query_data": parsed_response,
                "query_name": query_name,
                "header": header,
                "sub_header": sub_header

            }

            # Add json container to json contents
            json_file_contents.append(tmp_json_container)

        print("\nWriting temp json for: " + input_text_file.replace(".txt", ""))
        map_file = open(tmp_file_path + input_text_file.replace(".txt", "") + ".json", "w")
        map_file.write(json.dumps(json_file_contents, indent=4, sort_keys=True))
        map_file.close()

    # Parse stored data
    print("\nLoading temp data files")
    report_write_time = time.localtime(time.time())
    temp_json_files = load_json_files(tmp_file_path)
    parse_temp_data_files(temp_json_files, output_file_path, report_write_time)

    #Computing run time
    end = time.time()
    print("\n\n\nCrawler ran in : " + str(end - start))


def parse_temp_data_files(temp_json_files, output_file_path, report_write_time):
    # It should be noted, this method is a little manual, it can parse two payload types from new relic
    # all other payloads return errors, see read me for more.
    # It should also be noted, this parser creates very specific csv's which may not be reasonable for all use cases
    for temp_json_file in temp_json_files:

        # Set up csv fields and rows for csv writer
        csv_fields = ["key", "value"]
        csv_rows = []

        json_file_contents = temp_json_files.get(temp_json_file)

        for entry in json_file_contents:

            # Pull data from stored json
            header = entry.get("header")
            sub_header = entry.get("sub_header")
            query_data = entry.get("query_data")
            query_name = entry.get("query_name")
            response = query_data.get("response")

            # If a header exists, save it in a row on its own with an empty row above it
            if header:
                tmp = []
                csv_rows.append([])
                tmp.append(header)
                csv_rows.append(tmp)
                tmp = []

            # If response array has two entries we assume one is a "true" facet and one is a "false" facet, be sure to empty tmp
            if len(response) == 2:

                first_response = response[0]
                second_response = response[1]

                true_value = 0
                false_value = 0

                if first_response.get("facet") == "False":
                    false_value = first_response.get("count")
                if first_response.get("facet") == "True":
                    true_value = first_response.get("count")
                if second_response.get("facet") == "False":
                    false_value = second_response.get("count")
                if second_response.get("facet") == "True":
                    true_value = second_response.get("count")

                percentage = false_value / (false_value + true_value) * 100
                short_percentage = float("{:.3f}".format(percentage))

                # Store sub header and false percent to a row, be sure to empty tmp
                tmp = []
                tmp.append(sub_header)
                tmp.append(short_percentage)
                csv_rows.append(tmp)
                tmp = []

            # If response array has one entry it is either a score or a p90
            elif len(response) == 1:
                data_obj = response[0]
                keys = list(map(itemgetter(0), data_obj.items()))

                # For our purposes key length should always be one, but we check it anyway
                if len(keys) == 1:
                    response_data = data_obj.get(keys[0])

                    # If response data is a dict, we assume it is a p95 form
                    if type(response_data) is dict:
                        p50 = response_data.get("50")
                        p90 = response_data.get("90")
                        p95 = response_data.get("95")
                        p99 = response_data.get("99")

                        # Store empty row
                        tmp = []
                        csv_rows.append(tmp)

                        # Store sub header in row
                        tmp = []
                        tmp.append(sub_header)
                        csv_rows.append(tmp)

                        # Store p50 row
                        tmp = []
                        tmp.append("p50")
                        tmp.append(p50)
                        csv_rows.append(tmp)

                        # Store p90 row
                        tmp = []
                        tmp.append("p90")
                        tmp.append(p90)
                        csv_rows.append(tmp)

                        # Store p95 row
                        tmp = []
                        tmp.append("p95")
                        tmp.append(p95)
                        csv_rows.append(tmp)

                        # Store p99 row
                        tmp = []
                        tmp.append("p99")
                        tmp.append(p99)
                        csv_rows.append(tmp)

                    # If response data is a int, we assume it is a score form
                    elif type(response_data) is int:

                        # Store score row
                        tmp = []
                        tmp.append(sub_header)
                        tmp.append(response_data)
                        csv_rows.append(tmp)

                    else:
                        print(colored("We have error with data type, this means the response_data was NOT a dict or int", "red"))
                else:
                    print(colored("We have error with key size, this means we had more than one key, I am unclear on when this might happen", "red"))
            else:
                print(colored("We have error with response size, this means we have more than two entries in the response array, this may be ok, just not sure how to handle", "red"))

            # Write csv to file
            with open(output_file_path + "/" + temp_json_file + "_" + str(
                    time.strftime("%Z_%Y-%m-%d", report_write_time)) + ".csv",
                      'w') as csvfile:
                csvwriter = csv.writer(csvfile)
                csvwriter.writerow(csv_fields)
                csvwriter.writerows(csv_rows)


def load_txt_files(test_dir):
    # Pull every text file from input file path
    files = Path(test_dir).glob("**/*.txt")

    lines_by_files = {}

    # Load file line by line into a list of lists
    for txt_file in files:
        with open(txt_file) as txt_file:
            lines = txt_file.readlines()
            lines = [line.rstrip() for line in lines]
            lines_by_files[txt_file.name.split("/")[(len(txt_file.name.split("/")) - 1)]] = lines

    return lines_by_files


def load_json_files(test_dir):
    # Pull every json file from temp file path
    files = Path(test_dir).glob("**/*.json")

    json_by_files = {}

    # Load file parsing as json
    for json_file in files:
        with open(json_file) as json_file:
            data = json.load(json_file)
            json_by_files[json_file.name.split("/")[(len(json_file.name.split("/")) - 1)]] = data

    return json_by_files


def execute_rest_call(query, api, account, time_clause):
    # Base url for gql endpoint
    url = "https://api.newrelic.com/graphql"

    # Payload expects a gql string, except we are passing in sql into a gql object
    # Here we replace ACCOUNT with the account id, QUERY with the sql query and TIME with the time clause
    query_payload_string = "{actor {account(id: ACCOUNT) {nrql(query: \" QUERY TIME\") {results}}}}"
    query_payload_string = query_payload_string.replace("ACCOUNT", account)
    query_payload_string = query_payload_string.replace("QUERY", query)
    query_payload_string = query_payload_string.replace("TIME", time_clause)

    # Put query string inside standard json payload
    payload = {
        "query": query_payload_string
    }

    # Define headers
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "API-key": api
    }

    #Execute rest call
    response = requests.post(url, data=json.dumps(payload), headers=headers)
    return response


def parse_response(response):
    response_code = response.status_code
    response_data = {}

    if response_code == 200:
        print(colored("\t\tNew relic request returned", "yellow"))

        try:
            data = response.json()
            results = data.get("data").get("actor").get("account").get("nrql").get("results")
            response_data["response"] = results
            print(colored("\t\t\tNew relic rest call parsed", "green"))
        except:
            print(colored("\t\t\tNew relic response failed to parse", "red"))
            print(json.dumps(response.json(), sort_keys=False, indent=2))
            response_data["response"] = response.json()

    else:
        print(colored("\t\tNew relic request failed: " + str(response.json()), "red"))

    return response_data


if __name__ == "__main__":
    main()
