from argparse import ArgumentParser
from collections import defaultdict

from log_parser.handler import handle_info, stats
from log_parser.parser import LineInfo, ParseException, parse_line
from log_parser.stats import percentil_95


def parse_input(file_name: str) -> None:
    with open(file_name, 'r') as source:
        for line in source:
            try:
                line_info: LineInfo = parse_line(line)
                handle_info(line_info)
            except ParseException as exception:
                print(str(exception))


def print_result(file_name: str) -> None:
    requests_time_percentil = percentil_95(array=stats.durations)

    replica_groups = defaultdict(list)
    for backend, group_id in stats.backends_groups.items():
        replica_groups[group_id].append(backend)

    with open(file_name, 'w') as output:
        output.write(f'95-й перцентиль времени работы: {requests_time_percentil}\n')
        output.write(f'Идентификаторы запросов с самой долгой фазой отправки результатов пользователю:\n')

        for request_id in stats.longest_send_results_phase:
            output.write(f'    {request_id}\n')

        output.write(f'Запросов с неполным набором ответивших ГР: {stats.not_completed}\n')
        output.write('Обращения и ошибки по бэкандам\n')
        groups = sorted(replica_groups.keys())
        for group in groups:
            output.write(f'    ГР {group}\n')

            backends = sorted(replica_groups[group])
            for backend in backends:
                connections = stats.backends_connections[backend]

                output.write(f'        {backend}\n')
                output.write(f'            Обращения: {connections}\n')

                errors = stats.backends_errors[backend]
                if errors:
                    output.write(f'            Ошибки\n')
                for error, count in errors.items():
                    output.write(f'                {error}: {count}\n')


def main():
    argparser = ArgumentParser()

    argparser.add_argument('-f', '--file', default='input.txt', help='Name of input file')
    argparser.add_argument('-o', '--output', default='output.txt', help='Name of output file')

    args = argparser.parse_args()

    parse_input(file_name=args.file)
    print_result(file_name=args.output)


if __name__ == '__main__':
    main()
