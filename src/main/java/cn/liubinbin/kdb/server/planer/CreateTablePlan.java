package cn.liubinbin.kdb.server.planer;

import cn.liubinbin.kdb.server.table.Column;

import java.util.List;

/**
 * @author liubinbin
 * @date 2024/08/15
 * @info Created by liubinbin on 2024/08/14.
 */
public class CreateTablePlan extends Plan{

    private String tableName;
    private List<Column> columns;

    public CreateTablePlan(String tableName, List<Column> columns) {
        super(PlanKind.CREATE_TABLE);
        this.tableName = tableName;
        this.columns = columns;
    }

    public String getTableName() {
        return tableName;
    }

    public List<Column> getColumns() {
        return columns;
    }

    @Override
    public String toString() {
        return "CreateTablePlan{" +
                "tableName='" + tableName + '\'' +
                ", columns=" + columns +
                '}';
    }
}
