package org.jetbrains.kotlinx.dataframe.impl.aggregation

import org.jetbrains.kotlinx.dataframe.AnyColumnReference
import org.jetbrains.kotlinx.dataframe.DataFrame
import org.jetbrains.kotlinx.dataframe.DataRow
import org.jetbrains.kotlinx.dataframe.RowExpression
import org.jetbrains.kotlinx.dataframe.Selector
import org.jetbrains.kotlinx.dataframe.aggregation.AggregateDsl
import org.jetbrains.kotlinx.dataframe.aggregation.ColumnsForAggregateSelectionDsl
import org.jetbrains.kotlinx.dataframe.aggregation.ColumnsForAggregateSelector
import org.jetbrains.kotlinx.dataframe.api.asSequence
import org.jetbrains.kotlinx.dataframe.api.canHaveNA
import org.jetbrains.kotlinx.dataframe.api.cast
import org.jetbrains.kotlinx.dataframe.api.isNA
import org.jetbrains.kotlinx.dataframe.api.rows
import org.jetbrains.kotlinx.dataframe.columns.ColumnPath
import org.jetbrains.kotlinx.dataframe.columns.ColumnSet
import org.jetbrains.kotlinx.dataframe.columns.UnresolvedColumnsPolicy
import org.jetbrains.kotlinx.dataframe.impl.DataFrameReceiver
import org.jetbrains.kotlinx.dataframe.impl.aggregation.receivers.AggregateInternalDsl
import org.jetbrains.kotlinx.dataframe.impl.columns.ComputedColumnReference
import org.jetbrains.kotlinx.dataframe.impl.columns.resolveSingle
import org.jetbrains.kotlinx.dataframe.impl.columns.toColumnSet
import org.jetbrains.kotlinx.dataframe.impl.commonType
import org.jetbrains.kotlinx.dataframe.impl.getListType
import org.jetbrains.kotlinx.dataframe.type
import org.jetbrains.kotlinx.dataframe.values
import kotlin.reflect.KType
import kotlin.reflect.full.withNullability

@PublishedApi
internal fun <T, V> AggregateInternalDsl<T>.yieldOneOrMany(
    path: ColumnPath,
    values: List<V>,
    type: KType,
    default: V? = null,
) {
    when (values.size) {
        0 -> yield(path, null, type.withNullability(true), default)
        1 -> yield(path, values[0], type, default)
        else -> yield(path, values, getListType(type), default)
    }
}

@JvmName("toColumnSetForAggregate")
internal fun <T, C> ColumnsForAggregateSelector<T, C>.toColumns(): ColumnSet<C> =
    toColumnSet {
        object : DataFrameReceiver<T>(it.df.cast(), UnresolvedColumnsPolicy.Fail), ColumnsForAggregateSelectionDsl<T> {}
    }

@PublishedApi
internal fun <T> AggregateDsl<T>.internal(): AggregateInternalDsl<T> = this as AggregateInternalDsl<T>

internal fun <T, C> AggregateInternalDsl<T>.columnValues(
    columns: ColumnsForAggregateSelector<T, C>,
    forceYieldLists: Boolean,
    dropNA: Boolean,
    distinct: Boolean,
) {
    val cols = df.getAggregateColumns(columns)
    val isSingle = cols.size == 1
    cols.forEach { col ->
        val path = getPath(col, isSingle)

        val effectiveDropNA = if (dropNA) col.canHaveNA else false
        // TODO: use Set for distinct values
        val values = when {
            effectiveDropNA && distinct -> col.asSequence().filter { !it.isNA }.distinct().toList()
            effectiveDropNA && !distinct -> col.values.filter { !it.isNA }
            distinct -> col.values().distinct()
            else -> col.toList()
        }

        if (forceYieldLists) {
            yield(path, values, getListType(col.type), col.default)
        } else {
            yieldOneOrMany(path, values, col.type, col.default)
        }
    }
}

internal fun <T, C> AggregateInternalDsl<T>.columnValues(
    columns: ColumnsForAggregateSelector<T, C>,
    reducer: Selector<DataFrame<T>, DataRow<T>?>,
) {
    val row = reducer(df, df)
    val cols = df.getAggregateColumns(columns)
    val isSingle = cols.size == 1
    cols.forEach { col ->
        val path = getPath(col, isSingle)
        val value = if (row != null) col[row] else null
        yield(path, value, col.type, col.default)
    }
}

/**
 * Evaluates [expression] on [row].
 *
 * A column accessor, [org.jetbrains.kotlinx.dataframe.columns.ColumnPath], or other column reference is resolved
 * against [row]. The selected cell is returned with that column's declared element type.
 * A computed column reference keeps the type declared for the computation.
 * A scalar result keeps [declaredType].
 * [AnyColumnReference.getValue] still reports a missing column.
 */
@PublishedApi
internal fun <T, V> rowExpressionValue(
    row: DataRow<T>,
    expression: RowExpression<T, V>,
    declaredType: KType,
): Pair<Any?, KType> {
    val raw = expression(row, row)
    if (raw !is AnyColumnReference) return raw to declaredType
    if (raw is ComputedColumnReference<*>) return raw.getValue(row) to raw.type

    val value = raw.getValue(row)
    val selectedType = raw.resolveSingle(row.df(), UnresolvedColumnsPolicy.Skip)?.type() ?: declaredType
    return value to selectedType
}

@PublishedApi
internal fun <T, V> AggregateInternalDsl<T>.withExpr(type: KType, path: ColumnPath, expression: RowExpression<T, V>) {
    val results = df.rows().map { rowExpressionValue(it, expression, type) }
    val values = results.map { it.first }
    // One type per row: scalar rows keep [type], reference rows contribute the selected column type.
    // An empty group has nothing to resolve, so the declared expression type stays in place.
    val resultType = if (results.isEmpty()) type else results.map { it.second }.commonType()
    yieldOneOrMany(path, values, resultType)
}
