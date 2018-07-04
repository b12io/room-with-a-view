#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import os
import psycopg2
import yaml


class RoomWithAViewCommand(object):
    help = '''Manages Redshift SQL views.'''

    def __init__(self):
        self.actions = {
            'sync': ('Syncs specific views (identified by the --view-names '
                     'or --file-names parameters)',
                     self.sync_views),
            'sync-all': ('Syncs all views in all .sql files in a set of '
                         'directories (identified by the --directories '
                         'parameter). The directory will be searched '
                         'recursively',
                         self.sync_all),
            'drop': ('Drops specific views (identified by the --view-names '
                     'or --file-names parameters)',
                     self.drop_views),
            'drop-all': ('Drops all views in all .sql files in a set of '
                         'directories (identified by the --directories '
                         'parameter). The directory will be searched '
                         'recursively',
                         self.drop_all),
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
                            metavar='VIEW-NAME', default=[],
                            help=('View names to manage.'))
        parser.add_argument('--file-names', type=str, nargs='*',
                            metavar='FILE-PATH', default=[],
                            help=('Paths to .sql files to manage.'))
        parser.add_argument('--connection', type=str, required=False,
                            default='default', help=(
                                'Name of the Redshift connection to use (or '
                                '"default", if not specified). The name must '
                                'match a connection in settings.yaml'))
        parser.add_argument('--directories', type=str, nargs='*',
                            metavar='DIRECTORY', default=['default'],
                            help=('Directory names to search for SQL files '
                                  '(or "default" if not specified). Names '
                                  'must match directories in settings.yaml'))
        parser.add_argument('--settings', type=str, required=False,
                            default='settings.yaml', help=(
                                'Location of the settings file (settings.yaml '
                                'by default)'))
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
                self.directories = [
                    settings['directories'][directory_name]
                    for directory_name in self.options.directories]
        except Exception as e:
            raise ValueError('Unable to read settings.yaml: {}'.format(str(e)))

    def handle(self):
        try:
            self.parse_args()
            self.dependency_graph = self.parse_dependency_graph()
            handler = self.actions[self.options.action][1]
            handler()
            self.conn.commit()
        except Exception as e:
            print(e)
        finally:
            if hasattr(self, 'conn'):
                self.conn.close()

    def get_views_from_file(self, filename):
        dependency_graph = {}
        self.parse_file(filename, dependency_graph)
        return dependency_graph.keys()

    def parse_file(self, filename, dependency_graph):
        with open(filename, 'r') as sql_file:
            sql_contents = sql_file.read()

            for statement in sql_contents.split(';'):
                view_name, view_body = (
                    self.parse_create_view_statement(statement))
                if view_name is not None:
                    dependency_graph[view_name] = DependencyGraphNode(
                        view_name, view_body)

    def parse_dependency_graph(self):
        dependency_graph = {}
        for directory in self.directories:
            for root, dirs, files in os.walk(directory):
                for basename in files:
                    if not basename.lower().endswith('.sql'):
                        continue
                    filename = os.path.join(root, basename)
                    self.parse_file(filename, dependency_graph)

        view_names = dependency_graph.keys()
        for node in dependency_graph.values():
            dependencies = self.get_view_dependencies(
                node.view_name, node.view_body, view_names)
            node.out_edges |= set(dependencies)
            for dependency in dependencies:
                dependency_graph[dependency].in_edges.add(node.view_name)
        return dependency_graph

    def drop_views(self):
        """ Drops one or more views from Redshift.

        Views to drop are specified with either the '--view-names' or the
        '--file-names' arguments.
        """
        view_names = self.options.view_names
        file_names = self.options.file_names
        if not view_names and not file_names:
            raise ValueError('Either --view-names or --file-names is required '
                             'for the "drop" action.')
        for file_name in file_names:
            view_names.extend(self.get_views_from_file(file_name))

        for view_name in view_names:
            if view_name not in self.dependency_graph:
                raise ValueError(
                    'unrecognized view name: {}'.format(view_name))

        for view_name in view_names:
            self.drop_node(self.dependency_graph[view_name])

    def drop_all(self):
        """ Drops all views within a set of directories from Redshift.

        Since we cascade our drops and only drop views that already exist,
        the order in which we drop views doesn't matter.
        """
        for node in self.dependency_graph.values():
            self.drop_node(node)

    def sync_views(self):
        """ Syncs one or more views with Redshift.

        Since deleting a view will cascade, we have to recreate all views
        dependent on it, in topological order. To identify the order in which
        views need to be recreated, we do a depth-first search over the
        dependency graph for each view we're syncing.
        """
        view_names = self.options.view_names
        file_names = self.options.file_names
        if not view_names and not file_names:
            raise ValueError('Either --view-names or --file-names is required '
                             'for the "drop" action.')

        for file_name in file_names:
            view_names.extend(self.get_views_from_file(file_name))

        for view_name in view_names:
            if view_name not in self.dependency_graph:
                print(self.dependency_graph.keys())
                raise ValueError(
                    'unrecognized view name: {}'.format(view_name))

        # First, build a graph containing only nodes reachable from the views
        # to sync.
        starting_nodes = [self.dependency_graph[view_name]
                          for view_name in view_names]
        subgraph_node_names = self.traverse_graph(starting_nodes,
                                                  dependency_order=False)
        subgraph = {}
        for node_name in subgraph_node_names:
            original_node = self.dependency_graph[node_name]
            new_node = DependencyGraphNode(node_name, original_node.view_body)
            subgraph[node_name] = new_node
            new_node.in_edges = set([edge for edge in original_node.in_edges
                                     if edge in subgraph_node_names])
            new_node.out_edges = set([edge for edge in original_node.out_edges
                                      if edge in subgraph_node_names])

        # Then, drop all the views.
        for node in starting_nodes:
            self.drop_node(node)

        # Finally, run Kahn's algorithm to recreate the subgraph in dependency
        # order.
        starting_nodes = [node for node in subgraph.values()
                          if not node.out_edges]
        self.traverse_graph(starting_nodes, graph=subgraph,
                            visit_function=self.create_node)

    def sync_all(self):
        """ Syncs all views to Redshift. """
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
            if active_node.view_name in visited_nodes:
                continue

            # Visit the node
            visited_nodes.add(active_node.view_name)
            if visit_function:
                visit_function(active_node)

            # Add neighboring nodes to the queue
            for view_name in active_node.in_edges:
                next_node = graph[view_name]
                unvisited_dependents = next_node.out_edges - visited_nodes
                if not dependency_order or not unvisited_dependents:
                    active_nodes.append(next_node)
        return visited_nodes

    def execute_sql(self, sql_statement):
        # print('EXECUTING: {}'.format(sql_statement))
        with self.conn.cursor() as cursor:
            cursor.execute(sql_statement)

    def drop_node(self, node):
        print('Dropping view: {}'.format(node.view_name))
        self.execute_sql('DROP VIEW IF EXISTS {} CASCADE;'.format(
            node.view_name))

    def create_node(self, node):
        print('Creating view: {}'.format(node.view_name))
        self.execute_sql('CREATE VIEW {} AS {};'.format(
            node.view_name, node.view_body))

    def drop_and_recreate_node(self, node):
        self.drop_node(node)
        self.create_node(node)

    def parse_create_view_statement(self, statement):
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
        if 'CREATE VIEW' in first_line.upper() or (
                'CREATE OR REPLACE VIEW' in first_line.upper()):
            view_name = self.get_view_name(first_line)
            view_body = '\n'.join(statement_lines[1:])
        return view_name, view_body

    def get_view_name(self, line):
        # TODO(dhaas): regex me
        view_name = (line
                     .replace('CREATE VIEW', '')
                     .replace('CREATE OR REPLACE VIEW', '')
                     .strip())
        if view_name.lower().endswith('as'):
            view_name = view_name[:-2]
        return view_name.strip()

    def get_view_dependencies(self, for_view_name, sql_statement, view_names):
        return [view_name for view_name in view_names
                if view_name in sql_statement and view_name != for_view_name]


class DependencyGraphNode(object):
    def __init__(self, view_name, view_body):
        self.in_edges = set()  # views that depend on this view
        self.out_edges = set()  # views that this view depends on
        self.view_name = view_name
        self.view_body = view_body

    def __repr__(self):
        return 'View {} (depends on {}) (depended on by {})'.format(
            self.view_name, ', '.join(self.out_edges),
            ', '.join(self.in_edges))


if __name__ == '__main__':
    RoomWithAViewCommand().handle()
