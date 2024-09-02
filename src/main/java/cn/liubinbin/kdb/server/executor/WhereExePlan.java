package cn.liubinbin.kdb.server.executor;

import cn.liubinbin.kdb.server.entity.KdbRow;
import cn.liubinbin.kdb.server.planer.BoolExpression;
import cn.liubinbin.kdb.server.planer.OperatorKind;
import cn.liubinbin.kdb.server.table.AbstTable;
import cn.liubinbin.kdb.server.table.Column;

import java.util.List;


/**
 * @author liubinbin
 * @date 2024/08/27
 */
public class WhereExePlan extends AbstrExePlan {

    AbstTable table;
    private List<BoolExpression> boolExpressions;
    private boolean isWhereAnd;

    public WhereExePlan(AbstrExePlan nextPlan, AbstTable table, List<BoolExpression> boolExpressions, boolean isWhereAnd) {
        this(ExePlanKind.WhereFilter, nextPlan, table, boolExpressions, isWhereAnd);
    }
    
    public WhereExePlan(ExePlanKind kind, AbstrExePlan nextPlan, AbstTable table, List<BoolExpression> boolExpressions, boolean isWhereAnd) {
        super(kind, nextPlan);
        this.table = table;
        this.boolExpressions = boolExpressions;
        this.isWhereAnd = isWhereAnd;
        setColumnIdx();
    }

    public void setColumnIdx() {
        List<Column> columns = table.getColumns();
        for (BoolExpression boolExpression : boolExpressions) {
            String columnName = boolExpression.getColumnName();
            for (int i = 0; i < columns.size(); i++) {
                if (columns.get(i).getColumnName().equals(columnName)) {
                    boolExpression.setColumnIdx(i);
                }
            }
        }
    }

    @Override
    public boolean hasMore() {
        return nextPlan.hasMore();
    }

    public boolean isPass(KdbRow row) {
        boolean isPaas = isWhereAnd;
        for (BoolExpression boolExpression : boolExpressions) {
            boolean temp = isWhereAnd;
            if (boolExpression.getOperator() == OperatorKind.EQUAL) {
                temp = row.getValues().get(boolExpression.getColumnIdx()).compareTo(boolExpression.getValue()) == 0;
            } else if (boolExpression.getOperator() == OperatorKind.GREATER_THAN) {
                temp = row.getValues().get(boolExpression.getColumnIdx()).compareTo(boolExpression.getValue()) > 0;
            } else if (boolExpression.getOperator() == OperatorKind.LESS_THAN) {
                temp = row.getValues().get(boolExpression.getColumnIdx()).compareTo(boolExpression.getValue()) < 0;
            } else if (boolExpression.getOperator() == OperatorKind.NOT_EQUAL) {
                temp = row.getValues().get(boolExpression.getColumnIdx()).compareTo(boolExpression.getValue()) != 0;
            }
            if (isWhereAnd) {
                isPaas = isPaas && temp;
            } else {
                isPaas = isPaas || temp;
            }
        }
        return isPaas;
    }

    @Override
    public KdbRow onNext() {
        while(nextPlan.hasMore()) {
            KdbRow tempRow = nextPlan.onNext();
            if (tempRow == null) {
                continue;
            } else {
                if (isPass(tempRow)) {
                    return tempRow;
                }
            }
        }
        return null;
    }
}
