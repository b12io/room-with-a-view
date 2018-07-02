# -*- coding: utf-8 -*-
import os
import psycopg2

"""Main module."""

SQL_DIRECTORY = '../../crowdsurfing/common/analytics/'
def parse_dependency_graph():
    dependency_graph = {}
    for root, dirs, files in os.walk(SQL_DIRECTORY):
        for basename in files:
            if not basename.endswith('.sql'):
                continue
            filename = os.path.join(root, basename)
            with open(filename, 'r') as sql_file:
                sql_contents = sql_file.read()

            for statement in sql_contents.split(';'):
                view_name, view_body = parse_create_view_statement(statement)
                if view_name is not None:
                    dependency_graph[view_name] = DependencyGraphNode(
                        view_name, view_body)

    view_names = dependency_graph.keys()
    for node in dependency_graph.values():
        dependencies = get_view_dependencies(node.view_name, node.view_body, view_names)
        node.out_edges |= set(dependencies)
        for dependency in dependencies:
            dependency_graph[dependency].in_edges.add(node.view_name)

    return dependency_graph

REDSHIFT_SETTINGS = {
    'host': 'data-warehouse.cqhy4a9kivor.us-east-1.redshift.amazonaws.com',
    'port': '5439',
    'user': 'awsuser',
    'password': 'SurfData2015',
    'dbname': 'production',
}
CONN = psycopg2.connect(**REDSHIFT_SETTINGS)
def execute_sql(sql_statement):
    print('EXECUTING: {}'.format(sql_statement))
    with CONN.cursor() as cursor:
        cursor.execute(sql_statement)

def drop_and_recreate_node(node):
    drop_sql = 'DROP VIEW IF EXISTS {} CASCADE;'.format(node.view_name)
    create_sql = 'CREATE VIEW {} AS {};'.format(node.view_name, node.view_body)
    execute_sql(drop_sql)
    execute_sql(create_sql)

def sync_all_views(dependency_graph):
    visited_nodes = set()
    active_nodes = [node for node in dependency_graph.values() if not node.out_edges]
    #print([n.view_name for n in active_nodes])
    #print(dependency_graph['client_business_categories'])
    while active_nodes:
        active_node = active_nodes[0]
        active_nodes = active_nodes[1:]
        if active_node.view_name in visited_nodes:
            continue
        visited_nodes.add(active_node.view_name)
        drop_and_recreate_node(active_node)
        for view_name in active_node.in_edges:
            next_node = dependency_graph[view_name]
            # print(next_node.view_name, next_node.out_edges, visited_nodes)
            if not (dependency_graph[view_name].out_edges - visited_nodes):
                active_nodes.append(next_node)


class DependencyGraphNode(object):
    def __init__(self, view_name, view_body):
        self.in_edges = set()  # views that depend on this view
        self.out_edges = set()  # views that this view depends on
        self.view_name = view_name
        self.view_body = view_body

    def __repr__(self):
        return 'View {} (depends on {}) (depended on by {})'.format(
            self.view_name, ', '.join(self.out_edges), ', '.join(self.in_edges))


def parse_create_view_statement(statement):
    view_name = None
    view_body = None

    if not statement:
        return view_name, view_body

    # remove comments
    statement_lines = [line for line in statement.split('\n')
                       if line and not line.strip().startswith('--')]
    if not statement_lines:
        return view_name, view_body

    first_line = statement_lines[0]
    if ('CREATE VIEW' in first_line.upper() or
        'CREATE OR REPLACE VIEW' in first_line.upper()):
        view_name = get_view_name(first_line)
        view_body = '\n'.join(statement_lines[1:])
    return view_name, view_body

def get_view_name(line):
    # TODO(dhaas): regex me
    view_name = (line
                 .replace('CREATE VIEW', '')
                 .replace('CREATE OR REPLACE VIEW', '')
                 .strip())
    if view_name.lower().endswith('as'):
        view_name = view_name[:-2]
    return view_name.strip()

def get_view_dependencies(for_view_name, sql_statement, view_names):
    return [view_name for view_name in view_names
            if view_name in sql_statement and view_name != for_view_name]

# --views annotated_clients
def main():
    # Look through our SQL and parse out the dependency tree
    # Run SQL queries necessary to sync View(s) (DFS)
    # On failure, abort and tell us what failed

    dependency_graph = parse_dependency_graph()
    import pprint
    # pprint.pprint(list(dependency_graph.values()))
    sync_all_views(dependency_graph)
main()
