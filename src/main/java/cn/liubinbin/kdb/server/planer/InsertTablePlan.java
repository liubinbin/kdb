package cn.liubinbin.kdb.server.planer;

import cn.liubinbin.kdb.server.entity.KdbRow;

import java.util.List;

/**
 * @author liubinbin
 * @info Created by liubinbin on 2024/08/16.
 */
public class InsertTablePlan extends Plan {

    private final String tableName;
    private List<String> columnList;
    private List<KdbRow> rowList;

    public InsertTablePlan(String tableName) {
        super(PlanKind.INSERT_TABLE);
        this.tableName = tableName;
    }

    public InsertTablePlan(String tableName, List<String> columnList) {
        super(PlanKind.INSERT_TABLE);
        this.tableName = tableName;
        this.columnList = columnList;
    }

    public InsertTablePlan(String tableName, List<String> columnList, List<KdbRow> rowList) {
        super(PlanKind.INSERT_TABLE);
        this.tableName = tableName;
        this.columnList = columnList;
        this.rowList = rowList;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getColumnList() {
        return columnList;
    }

    public List<KdbRow> getRowList() {
        return rowList;
    }


    @Override
    public String toString() {
        return "InsertTablePlan{" +
                "tableName='" + tableName + '\'' +
                ", columnList=" + columnList + '\'' +
                ", rowList=" + rowList + '\'' +
                '}';
    }
}
