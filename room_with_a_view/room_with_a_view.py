#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import os
import re
from operator import attrgetter

import psycopg2
import yaml


# Matches a view or function definition
SQL_VIEW_STATEMENT_RE = re.compile(
    r'(?P<declaration>'
    'create\s+(or replace\s+)?'  # Matches 'create [or replace]'
    '(?P<type>view)\s+'          # Matches the 'view' keyword
    '(?P<name>\w+)'              # Matches the name of the view
    '.+?(?=as)'                  # Matches everything before the next 'as'
    'as)(?P<body>.*)',           # Matches 'as', followed by the view body
    re.I | re.S | re.M)          # Case insensitive, multi-line matching.


SQL_FUNCTION_STATEMENT_RE = re.compile(
    r'(?P<declaration>'
    'create\s+(or replace\s+)?'  # Matches 'create [or replace]'
    '(?P<type>function)\s+'      # Matches the 'function' keyword
    '(?P<name>\w+)\s*'           # Matches the name of the function
    '(?P<arg_list>\(.*\))'       # Matches the function's argument list
    '[^)]+?(?=returns)'          # Matches everything before the word 'returns'
    'returns.+?(?=as)'           # Matches everything before the word 'as'
    'as\s+\$\$)'                 # Matches 'as $$'
    '(?P<body>.*)',              # Matches everything else in the statement
    re.I | re.S | re.M)          # Case insensitive, multi-line matching.


class RoomWithAViewCommand(object):
    help = '''Manages Redshift SQL views.'''

    def __init__(self):
        self.actions = {
            'sync': ('Syncs specific views or functions (identified by the '
                     '--view-names or --file-names parameters).',
                     self.sync_views),
            'sync-all': ('Syncs all views and functions in all .sql files in '
                         'a set of directories (identified by the '
                         '--directories parameter). The directory will be '
                         'searched recursively.',
                         self.sync_all),
            'drop': ('Drops specific views or functions (identified by the '
                     '--view-names or --file-names parameters).',
                     self.drop_views),
            'drop-all': ('Drops all views and functions in all .sql files in '
                         'a set of directories (identified by the '
                         '--directories parameter). The directory will be '
                         'searched recursively.',
                         self.drop_all),
            'list': ('lists all known views and functions.', self.list_all),
        }

    def parse_args(self):
        actions_help = 'Possible actions:\n\t{}'.format(
            '\n\t'.join(['{}: {}'.format(action, description)
                         for action, (description, _)
                         in self.actions.items()]))
        parser = argparse.ArgumentParser(
            description='{} {}'.format(self.help, actions_help),
            formatter_class=argparse.RawDescriptionHelpFormatter)
        parser.add_argument('action', type=str, choices=self.actions.keys(),
                            help='The action to perform.')
        parser.add_argument('--view-names', type=str, nargs='*',
                            metavar='VIEW-OR-FUNCTION-NAME', default=[],
                            help=('Names of views or functions to which to '
                                  'apply the action.'))
        parser.add_argument('--file-names', type=str, nargs='*',
                            metavar='FILE-PATH', default=[],
                            help=('Paths to .sql files to which to apply the '
                                  'action.'))
        parser.add_argument('--connection', type=str, required=False,
                            default='default', help=(
                                'Name of the Redshift connection to use (or '
                                '"default", if not specified). The name must '
                                'match a connection in settings.yaml'))
        parser.add_argument('--settings', type=str, required=False,
                            default='settings.yaml', help=(
                                'Location of the settings file (settings.yaml '
                                'by default)'))
        parser.add_argument('--verbosity', type=int, required=False, default=1,
                            help=('Verbosity of script output. 0 will output '
                                  'nothing, 1 will output names of views and '
                                  'functions being dropped and created, and 2 '
                                  'will output all executed sql'))
        self.options = parser.parse_args()

        try:
            with open(self.options.settings, 'r') as stream:
                settings = yaml.load(stream)
                connection_options = settings['connections'].get(
                    self.options.connection)
                if not connection_options:
                    raise ValueError('Unrecognized connection name: {}'.format(
                        self.options.connection))
                self.conn = psycopg2.connect(**connection_options)
                self.directories = settings['directories']
        except Exception as e:
            raise ValueError('Unable to read settings.yaml: {}'.format(str(e)))

    def handle(self):
        try:
            self.parse_args()
            self.dependency_graph = self.parse_dependency_graph()
            handler = self.actions[self.options.action][1]
            handler()
            self.conn.commit()
        finally:
            if hasattr(self, 'conn'):
                self.conn.close()

    def list_all(self):
        sorted_graph = sorted(
            self.dependency_graph.values(), key=attrgetter('name'))
        print('Known views and functions:\n\n{}'.format('\n\n'.join([
            str(node) for node in sorted_graph])))

    def get_statements_from_file(self, filename):
        dependency_graph = {}
        self.parse_file(filename, dependency_graph)
        return dependency_graph.keys()

    def parse_file(self, filename, dependency_graph):
        with open(filename, 'r') as sql_file:
            sql_contents = sql_file.read()

        for statement in sql_contents.split(';'):
            statement_data = self.parse_statement(statement)
            if statement_data['name'] is not None:
                dependency_graph[statement_data['name']] = DependencyGraphNode(
                    **statement_data)

    def parse_dependency_graph(self):
        dependency_graph = {}
        for directory in self.directories:
            for root, dirs, files in os.walk(directory):
                for basename in files:
                    if not basename.lower().endswith('.sql'):
                        continue
                    filename = os.path.join(root, basename)
                    self.parse_file(filename, dependency_graph)

        statement_names = dependency_graph.keys()
        for node in dependency_graph.values():
            dependencies = self.get_dependencies(
                node.name, node.body, statement_names)
            node.out_edges |= set(dependencies)
            for dependency in dependencies:
                dependency_graph[dependency].in_edges.add(node.name)
        return dependency_graph

    def get_statements_from_arguments(self):
        statement_names = self.options.view_names
        file_names = self.options.file_names
        if not statement_names and not file_names:
            raise ValueError(
                'Either --view-names or --file-names is required.')

        for file_name in file_names:
            statement_names.extend(self.get_statements_from_file(file_name))

        for statement_name in statement_names:
            if statement_name not in self.dependency_graph:
                raise ValueError('unrecognized view or function name: {}'
                                 .format(statement_name))
        return statement_names

    def drop_views(self):
        """ Drops one or more view or functions from Redshift.

        Views and functions to drop are specified with either the
        '--view-names' or the '--file-names' arguments.
        """
        statement_names = self.get_statements_from_arguments()
        for statement_name in statement_names:
            self.drop_node(self.dependency_graph[statement_name])

    def drop_all(self):
        """ Drops all views and functions from Redshift.

        Since we cascade our drops and only drop views and functions that
        already exist, the order in which we drop views doesn't matter.
        """
        for node in self.dependency_graph.values():
            self.drop_node(node)

    def sync_views(self):
        """ Syncs one or more views or functions with Redshift.

        Since deleting a view will cascade, we have to recreate all views
        dependent on it, in topological order. To identify the order in which
        views need to be recreated, we build a subgraph consisting of only
        views that can be reached from the views we're syncing, then recreate
        the views in topological order on the subgraph using Kahn's algorithm
        (as we do in ``sync_all()``).
        """
        statement_names = self.get_statements_from_arguments()

        # First, build a graph containing only nodes reachable from the views
        # and functions to sync.
        starting_nodes = [self.dependency_graph[statement_name]
                          for statement_name in statement_names]
        subgraph_node_names = self.traverse_graph(starting_nodes,
                                                  dependency_order=False)
        subgraph = {}
        for node_name in subgraph_node_names:
            original_node = self.dependency_graph[node_name]
            new_node = DependencyGraphNode(
                name=original_node.name,
                declaration=original_node.declaration,
                statement_type=original_node.statement_type,
                body=original_node.body,
                comments=original_node.comments,
                arg_list=original_node.arg_list)
            subgraph[node_name] = new_node
            new_node.in_edges = set([edge for edge in original_node.in_edges
                                     if edge in subgraph_node_names])
            new_node.out_edges = set([edge for edge in original_node.out_edges
                                      if edge in subgraph_node_names])

        # Then, drop all the views and functions.
        for node in starting_nodes:
            self.drop_node(node)

        # Finally, run Kahn's algorithm to recreate the subgraph in dependency
        # order.
        starting_nodes = [node for node in subgraph.values()
                          if not node.out_edges]
        self.traverse_graph(starting_nodes, graph=subgraph,
                            visit_function=self.create_node)

    def sync_all(self):
        """ Syncs all views and functions to Redshift. """
        starting_nodes = [node for node in self.dependency_graph.values()
                          if not node.out_edges]
        self.traverse_graph(
            starting_nodes, visit_function=self.drop_and_recreate_node)

    def traverse_graph(self, starting_nodes, graph=None, visit_function=None,
                       dependency_order=True):
        """ Traverses a dependency graph and returns the visited nodes' names.

        This is a breadth first search over the graph.
        :param starting_nodes: the set of nodes from which to run the search.
        :param graph: the graph to search over.
        :param visit_function: A function to run at each node visited.
        :param dependency_order: if True, nodes will be visited in a
          topological order guaranteed not to violate dependencies. This is
          enforced using Kahn's algorithm (read more here:
          https://en.wikipedia.org/wiki/Topological_sorting#Kahn's_algorithm).
        :returns: A set of visited node names.
        """
        graph = graph or self.dependency_graph
        visited_nodes = set()
        active_nodes = starting_nodes
        while active_nodes:
            # Pop a new node off the queue
            active_node = active_nodes[0]
            active_nodes = active_nodes[1:]
            if active_node.name in visited_nodes:
                continue

            # Visit the node
            visited_nodes.add(active_node.name)
            if visit_function:
                visit_function(active_node)

            # Add neighboring nodes to the queue
            for node_name in active_node.in_edges:
                next_node = graph[node_name]
                unvisited_dependents = next_node.out_edges - visited_nodes
                if not dependency_order or not unvisited_dependents:
                    active_nodes.append(next_node)
        return visited_nodes

    def execute_sql(self, sql_statement):
        if self.options.verbosity >= 2:
            print('Executing: {}'.format(sql_statement))
        with self.conn.cursor() as cursor:
            cursor.execute(sql_statement)
            if cursor.rowcount >= 1:
                return cursor.fetchall()

    def drop_node(self, node):
        if self.options.verbosity >= 1:
            print('Dropping {}: {}'.format(node.statement_type, node.name))
        if node.statement_type == 'view':
            sql = 'DROP VIEW IF EXISTS {} CASCADE'.format(node.name)
        elif node.statement_type == 'function':
            # There's no 'Drop if exists' for functions in redshift, so we
            # first look up the function in the system catalog.
            result = self.execute_sql(
                "select proname from pg_proc where proname = '{}'"
                .format(node.name))
            if not result or result[0] != node.name:
                return  # function doesn't exist, no need to drop

            sql = 'DROP FUNCTION {} {} CASCADE'.format(
                node.name, node.arg_list)
        else:
            raise ValueError('Unrecognized node type: {}'.format(
                node.statement_type))
        self.execute_sql(sql)

    def create_node(self, node):
        if self.options.verbosity >= 1:
            print('Creating {}: {}'.format(node.statement_type, node.name))
        self.execute_sql(''.join([node.declaration, node.body]))

    def drop_and_recreate_node(self, node):
        self.drop_node(node)
        self.create_node(node)

    def parse_statement(self, statement):
        statement_data = {'name': None}

        if not statement:
            return statement_data

        # Extract comments from above the declaration.
        statement_lines = [line for line in statement.splitlines() if line]
        comments = []
        for line in statement_lines:
            if line.strip().startswith('--'):
                comments.append(line.lstrip('- \t'))
            else:
                break
        statement_data['comments'] = ' '.join(comments)

        # remove comments
        sql_lines = [line for line in statement_lines
                     if not line.strip().startswith('--')]
        if not sql_lines:
            return statement_data

        # Find view or function declarations
        raw_statement = '\n'.join(sql_lines)
        match = SQL_VIEW_STATEMENT_RE.fullmatch(raw_statement)
        if not match:
            match = SQL_FUNCTION_STATEMENT_RE.fullmatch(raw_statement)
            if not match:
                return statement_data
        statement_data.update({
            'statement_type': match.group('type').lower(),
            'name': match.group('name'),
            'declaration': match.group('declaration'),
            'body': match.group('body'),
        })
        statement_data['arg_list'] = (
            match.group('arg_list')
            if statement_data['statement_type'] == 'function' else None)
        return statement_data

    def get_dependencies(self, cur_statement_name, cur_statement_body,
                         all_statement_names):
        return [statement_name for statement_name in all_statement_names
                if statement_name in cur_statement_body
                and statement_name != cur_statement_name]


class DependencyGraphNode(object):
    def __init__(self, **kwargs):
        self.in_edges = set()  # views that depend on this view
        self.out_edges = set()  # views that this view depends on
        for k, v in kwargs.items():
            setattr(self, k, v)

    def __repr__(self):
        description = '\n\t'.join([
            'description: {}'.format(self.comments),
            'depends on: {}'.format(', '.join(self.out_edges)),
            'depended on by: {}'.format(', '.join(self.in_edges))])
        return '{} {}:\n\t{}'.format(
            self.statement_type.title(), self.name, description)


if __name__ == '__main__':
    RoomWithAViewCommand().handle()
