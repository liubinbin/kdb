package cn.liubinbin.kdb.server.planer;

import java.util.List;

/**
 * @author liubinbin
 * @info Created by liubinbin on 2024/08/17.
 */
public class SelectTablePlan extends Plan {

    private final String tableName;
    private boolean isStar;
    private List<String> columnList;
    private List<BoolExpression> whereBoolExpreList;
    private boolean isWhereAnd;
    private boolean isOrderBy;
    private String columnOrderBy;
    private Integer limit;

    public SelectTablePlan(String tableName, List<String> columnList, boolean isWhereAnd, List<BoolExpression> whereBoolExpreList) {
        super(PlanKind.SELECT_TABLE);
        this.tableName = tableName;
        this.columnList = columnList;
        if (columnList.isEmpty()) {
            this.isStar = true;
        }
        this.isWhereAnd = isWhereAnd;
        this.whereBoolExpreList = whereBoolExpreList;
        this.isOrderBy = false;
    }

    public SelectTablePlan(String tableName, List<String> columnList, boolean isWhereAnd, List<BoolExpression> whereBoolExpreList,
                           String columnOrderBy, Integer limit){
        this(tableName, columnList, isWhereAnd, whereBoolExpreList);
        this.isOrderBy = true;
        this.columnOrderBy = columnOrderBy;
        this.limit = limit;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getColumnList() {
        return columnList;
    }

    @Override
    public String toString() {
        return "SelectTablePlan{" +
                "tableName='" + tableName + '\'' +
                ", isStar=" + isStar +
                ", columnList=" + columnList +
                ", whereBoolExpreList=" + whereBoolExpreList +
                ", isWhereAnd=" + isWhereAnd +
                ", isOrderBy=" + isOrderBy +
                ", columnOrderBy='" + columnOrderBy + '\'' +
                ", limit=" + limit +
                '}';
    }
}
