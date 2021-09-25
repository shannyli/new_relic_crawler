import argparse
from termcolor import colored
import requests
import json
from pathlib import Path
import time
import csv
from operator import itemgetter
import time


def main():
    start = time.time()
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

    # Load up input vars
    args = arg_parser.parse_args()
    api_key = args.apiKey
    account_id = args.accountId
    file_path = args.inputFile
    json_path = args.jsonFile
    output_path = args.outputFile

    print("\nStarting New Relic Crawler"
          "\n\tAPI KEY: " + str(api_key) +
          "\n\tACCOUNT KEY: " + str(account_id) +
          "\n\tINPUT: " + file_path + "\n")

    lines_by_files = load_txt_files(file_path)

    #Generate Data
    for file_lines in lines_by_files:
        output_for_file = []
        print(colored("\n\nName of file: " + str(file_lines), "yellow"))
        lines = lines_by_files.get(file_lines)
        for line in lines:
            line_sections = line.split("|")
            header = line_sections[0]
            sub_header = line_sections[1]
            query_name = line_sections[2]
            query = line_sections[3]
            time_clause = line_sections[4]
            query_sql_safe = query #sql_safe(query)

            print("Running query: " + str(colored(query_name, "blue")) + " with sql: " + str(
                colored(query_sql_safe, "magenta")) + " with time " + str(colored(time_clause, "yellow")))

            response = execute_rest_call(query, api_key, account_id, time_clause)
            parsed_response = parse_response(query_name, response)
            data_holder = {
                "query_data": parsed_response,
                "query_name": query_name,
                "header": header,
                "sub_header": sub_header


            }
            output_for_file.append(data_holder)

        print("\nwriting file for " + file_lines.replace(".txt", ""))
        map_file = open("json_out/" + file_lines.replace(".txt", "") + ".json", "w")
        map_file.write(json.dumps(output_for_file, indent=4, sort_keys=True))
        map_file.close()

    #Parse Data
    print("\n\nLOADING DATA FOR CSV GENERATION")
    report_write_time = time.localtime(time.time())
    files = load_json_files(json_path)
    for file in files:
        csv_fields = ["key", "value"]
        csv_rows = []
        data = files.get(file)
        for entry in data:
            header = entry.get("header")
            sub_header = entry.get("sub_header")
            query_data = entry.get("query_data")
            query_name = entry.get("query_name")

            if header:
                tmp = []
                csv_rows.append([])
                tmp.append(header)
                csv_rows.append(tmp)
                tmp = []

            response = query_data.get("response")
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

                percentage = false_value / (false_value + true_value)
                short_percentage = float("{:.3f}".format(percentage))

                tmp = []
                tmp.append(sub_header)
                tmp.append(short_percentage)
                csv_rows.append(tmp)
                tmp = []

                # print(query_name + " has value of " + str(short_percentage))

            elif len(response) == 1:
                data_obj = response[0]
                keys = list(map(itemgetter(0), data_obj.items()))
                if len(keys) == 1:
                    response_data = data_obj.get(keys[0])
                    if type(response_data) is dict:
                        p50 = response_data.get("50")
                        p90 = response_data.get("90")
                        p95 = response_data.get("95")
                        p99 = response_data.get("99")
                        # print(query_name)
                        # print("\tp50: " + str(p50))
                        # print("\tp90: " + str(p90))
                        # print("\tp95: " + str(p95))
                        # print("\tp99: " + str(p99))

                        tmp = []
                        csv_rows.append(tmp)

                        tmp = []
                        tmp.append(sub_header)
                        csv_rows.append(tmp)

                        tmp = []
                        tmp.append("p50")
                        tmp.append(p50)
                        csv_rows.append(tmp)

                        tmp = []
                        tmp.append("p90")
                        tmp.append(p90)
                        csv_rows.append(tmp)

                        tmp = []
                        tmp.append("p95")
                        tmp.append(p95)
                        csv_rows.append(tmp)

                        tmp = []
                        tmp.append("p99")
                        tmp.append(p99)
                        csv_rows.append(tmp)

                    elif type(response_data) is int:

                        tmp = []
                        tmp.append(sub_header)
                        tmp.append(response_data)
                        csv_rows.append(tmp)

                        # print(sub_header + " has value of " + str(response_data))




                    else:
                        print(colored("we have error with data type", "red"))
                else:
                    print(colored("we have error with key size", "red"))
            else:
                print(colored("we have error with response size", "red"))

            with open(output_path + "/" + file + "_" + str(
                    time.strftime("%Z_%Y-%m-%d", report_write_time)) + ".csv",
                      'w') as csvfile:
                csvwriter = csv.writer(csvfile)
                csvwriter.writerow(csv_fields)
                csvwriter.writerows(csv_rows)
    end = time.time()
    print("\n\n\nCrawler ran in : " + str(end - start))


def load_txt_files(test_dir):
    files = Path(test_dir).glob("**/*.txt")

    lines_by_files = {}

    for txt_file in files:
        with open(txt_file) as txt_file:
            lines = txt_file.readlines()
            lines = [line.rstrip() for line in lines]
            lines_by_files[txt_file.name.split("/")[(len(txt_file.name.split("/")) - 1)]] = lines

    return lines_by_files


def load_json_files(test_dir):
    files = Path(test_dir).glob("**/*.json")

    lines_by_files = {}

    for txt_file in files:
        with open(txt_file) as json_file:
            data = json.load(json_file)
            lines_by_files[txt_file.name.split("/")[(len(txt_file.name.split("/")) - 1)]] = data

    return lines_by_files


def sql_safe(query):
    query_strings = query.split(" ")
    for idx, val in enumerate(query_strings):
        if "-" in val:
            val = "`" + val + "`"
            query_strings[idx] = val

    return ' '.join(query_strings)


def execute_rest_call(query, api, account, time_clause):
    url = "https://api.newrelic.com/graphql"
    query_payload_string = "{actor {account(id: ACCOUNT) {nrql(query: \" QUERY TIME\") {results}}}}"
    query_payload_string = query_payload_string.replace("ACCOUNT", account)
    query_payload_string = query_payload_string.replace("QUERY", query)
    query_payload_string = query_payload_string.replace("TIME", time_clause)

    payload = {
        "query": query_payload_string
    }

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "API-key": api
    }
    response = requests.post(url, data=json.dumps(payload), headers=headers)
    return response


def parse_response(query_name, response):
    response_code = response.status_code
    response_data = {}
    if response_code == 200:
        print(colored("\tNew relic response success", "green"))

        try:
            data = response.json()
            results = data.get("data").get("actor").get("account").get("nrql").get("results")
            response_data["response"] = results
            # print(json.dumps(results, sort_keys=False, indent=2))
        except:
            print(colored("\t\tNew relic response failed to parse", "red"))
            print(json.dumps(response.json(), sort_keys=False, indent=2))
            response_data["response"] = response.json()

    else:
        print(colored("\tNew relic response failed: " + str(response.json()), "red"))

    return response_data


if __name__ == "__main__":
    main()
