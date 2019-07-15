from __future__ import absolute_import
from __future__ import unicode_literals

import django
from django.db import connections
from django.db.models.sql import DeleteQuery, Query, UpdateQuery
from django.db.models.sql.compiler import (
    SQLCompiler,
    SQLDeleteCompiler,
    SQLUpdateCompiler,
)


class CTEQuery(Query):
    """A Query which processes SQL compilation through the CTE compiler"""

    def __init__(self, *args, **kwargs):
        super(CTEQuery, self).__init__(*args, **kwargs)
        self._with_ctes = []

    @classmethod
    def from_query(cls, other):
        new = cls(
            model=other.model,
            where=other.where_class
        )

        new.model = other.model
        new.alias_refcount = other.alias_refcount
        new.alias_map = other.alias_map
        new.external_aliases = other.external_aliases
        new.table_map = other.table_map
        new.default_cols = other.default_cols
        new.default_ordering = other.default_ordering
        new.standard_ordering = other.standard_ordering
        new.used_aliases = other.used_aliases
        new.filter_is_sticky = other.filter_is_sticky

        new.select = other.select
        new.tables = other.tables
        new.where = other.where

        new.where_class = other.where_class
        new.group_by = other.group_by
        new.order_by = other.order_by
        new.low_mark = other.low_mark
        new.high_mark = other.high_mark
        new.distinct = other.distinct
        new.distinct_fields = other.distinct_fields
        new.select_for_update = other.select_for_update
        new.select_for_update_nowait = other.select_for_update_nowait

        new.select_related = other.select_related
        new.max_depth = other.max_depth

        new.values_select = other.values_select

        new._annotations = other._annotations
        new.annotation_select_mask = other.annotation_select_mask
        new._annotation_select_cache = other._annotation_select_cache

        new._extra = other._extra
        new.extra_select_mask = other.extra_select_mask
        new._extra_select_cache = other._extra_select_cache

        new.extra_tables = other.extra_tables
        new.extra_order_by = other.extra_order_by

        new.deferred_loading = other.deferred_loading

        new.context = other.context

        return new

    def combine(self, other, connector):
        if other._with_ctes:
            if self._with_ctes:
                raise TypeError("cannot merge queries with CTEs on both sides")
            self._with_ctes = other._with_ctes[:]
        return super(CTEQuery, self).combine(other, connector)

    def get_compiler(self, using=None, connection=None):
        """ Overrides the Query method get_compiler in order to return
            a CTECompiler.
        """
        # Copy the body of this method from Django except the final
        # return statement. We will ignore code coverage for this.
        if using is None and connection is None:  # pragma: no cover
            raise ValueError("Need either using or connection")
        if using:
            connection = connections[using]
        # Check that the compiler will be able to execute the query
        for alias, aggregate in self.annotation_select.items():
            connection.ops.check_expression_support(aggregate)
        # Instantiate the custom compiler.
        klass = COMPILER_TYPES.get(self.__class__, CTEQueryCompiler)
        return klass(self, connection, using)

    def __chain(self, _name, klass=None, *args, **kwargs):
        klass = QUERY_TYPES.get(klass, self.__class__)
        clone = getattr(super(CTEQuery, self), _name)(klass, *args, **kwargs)
        clone._with_ctes = self._with_ctes[:]
        return clone

    if django.VERSION < (2, 0):
        def clone(self, klass=None, *args, **kwargs):
            return self.__chain("clone", klass, *args, **kwargs)

    else:
        def chain(self, klass=None):
            return self.__chain("chain", klass)


class CTECompiler(object):

    TEMPLATE = "{name} AS ({query})"

    @classmethod
    def generate_sql(cls, connection, query, as_sql):
        if getattr(query, 'combinator', False):
            return as_sql()

        ctes = []
        params = []
        for cte in query._with_ctes:
            compiler = cte.query.get_compiler(connection=connection)
            cte_sql, cte_params = compiler.as_sql()
            ctes.append(cls.TEMPLATE.format(name=cte.name, query=cte_sql))
            params.extend(cte_params)

        # Always use WITH RECURSIVE
        # https://www.postgresql.org/message-id/13122.1339829536%40sss.pgh.pa.us
        sql = ["WITH RECURSIVE", ", ".join(ctes)] if ctes else []
        base_sql, base_params = as_sql()
        sql.append(base_sql)
        params.extend(base_params)
        return " ".join(sql), tuple(params)


class CTEUpdateQuery(UpdateQuery, CTEQuery):
    pass


class CTEDeleteQuery(DeleteQuery, CTEQuery):
    pass


QUERY_TYPES = {
    UpdateQuery: CTEUpdateQuery,
    DeleteQuery: CTEDeleteQuery,
}


class CTEQueryCompiler(SQLCompiler):

    def as_sql(self, *args, **kwargs):
        def _as_sql():
            return super(CTEQueryCompiler, self).as_sql(*args, **kwargs)
        return CTECompiler.generate_sql(self.connection, self.query, _as_sql)


class CTEUpdateQueryCompiler(SQLUpdateCompiler):

    def as_sql(self, *args, **kwargs):
        def _as_sql():
            return super(CTEUpdateQueryCompiler, self).as_sql(*args, **kwargs)
        return CTECompiler.generate_sql(self.connection, self.query, _as_sql)


class CTEDeleteQueryCompiler(SQLDeleteCompiler):

    # NOTE: it is currently not possible to execute delete queries that
    # reference CTEs without patching `QuerySet.delete` (Django method)
    # to call `self.query.chain(sql.DeleteQuery)` instead of
    # `sql.DeleteQuery(self.model)`

    def as_sql(self, *args, **kwargs):
        def _as_sql():
            return super(CTEDeleteQueryCompiler, self).as_sql(*args, **kwargs)
        return CTECompiler.generate_sql(self.connection, self.query, _as_sql)


COMPILER_TYPES = {
    CTEUpdateQuery: CTEUpdateQueryCompiler,
    CTEDeleteQuery: CTEDeleteQueryCompiler,
}
