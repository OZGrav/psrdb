from base64 import b64encode


class GraphQLClause:
    def __init__(self, field, value, join):
        # Generate a ID encoded join if a join table is specified AND we have a field that ends in ID
        if not join is None and (field.lower().endswith("id") or type(value) == int):
            value = b64encode(f"{join}Node:{value}".encode("ascii")).decode("utf-8")
        if type(value) == str:
            self.clause = f'{field}: "{value}"'
        else:
            self.clause = f"{field}: {value}"

    def get(self):
        return self.clause


class GraphQLQuery:
    """Generate a GraphQL Query supporting features such as pagination"""

    def __init__(self, op_name):
        self.op_type = "query"
        self.op_name = op_name
        self.field_list = ["id"]
        self.delim = ",\n                        "
        self.cursor = None
        self.clauses = []
        self.use_pagination = False
        self.page_info = """pageInfo {
                    endCursor,
                    hasNextPage
                }
        """

        self.template = """
        %s %s {
            %s {
                edges {
                    node {
                        %s
                    }
                }
                %s
            }
        }
        """

    def add_clause(self, clause):
        """Add a clause to the query"""
        self.clauses.append(clause.get())

    def set_field_list(self, _field_list):
        self.field_list = _field_list

    def set_use_pagination(self, use):
        self.use_pagination = use

    def generate(self, pagination_clauses):
        all_clauses = self.clauses.copy()
        all_clauses.extend(pagination_clauses)
        query_clauses = self.op_name
        if len(all_clauses) > 0:
            query_clauses += "(" + ", ".join(all_clauses) + ")"
        fields = self.delim.join(self.field_list)

        if self.use_pagination:
            return self.template % (self.op_type, self.op_name, query_clauses, fields, self.page_info)
        else:
            return self.template % (self.op_type, self.op_name, query_clauses, fields, "")

    def paginate(self, cursor):
        clauses = ["first:100"]
        if not cursor is None:
            clauses.append('after:"' + cursor + '"')
        return self.generate(clauses)

    @classmethod
    def build_clauses_from_args(cls, filters):
        clauses = []
        for f in filters:
            if not f["value"] is None:
                clauses.append(GraphQLClause(f["field"], f["value"], f["join"]))
        return clauses


class GraphQLQueryId(GraphQLQuery):
    def __init__(self, record_name, table_name, table_id):
        GraphQLQuery.__init__(self, record_name.lower())
        self.add_clause(GraphQLClause("id", table_id, table_name.title()))

        # use a simpler query template
        self.template = """
        %s %s {
            %s {
                %s
            }
        }
        """
        self.delim = ",\n                "

    def generate(self, pagination_clauses):
        query_clauses = self.op_name
        if len(self.clauses) > 0:
            query_clauses += "(" + ", ".join(self.clauses) + ")"
        fields = self.delim.join(self.field_list)
        return self.template % (self.op_type, self.op_name, query_clauses, fields)

    def paginate(self, cursor):
        return self.generate([])


class GraphQLQueryAll(GraphQLQuery):
    def __init__(self, table_name):
        GraphQLQuery.__init__(self, f"all{table_name.title()}")


class GraphQLQueryClauses(GraphQLQuery):
    def __init__(self, table_name, clauses):
        GraphQLQuery.__init__(self, f"all{table_name.title()}")
        for clause in clauses:
            self.add_clause(clause)


def graphql_query_factory(table_name, record_name, id, filters):

    clauses = GraphQLQuery.build_clauses_from_args(filters)
    if id is not None:
        return GraphQLQueryId(record_name, table_name, id)
    elif len(clauses) > 0:
        return GraphQLQueryClauses(table_name, clauses)
    else:
        return GraphQLQueryAll(table_name)
