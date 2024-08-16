package cn.liubinbin.kdb.experiment;

import cn.liubinbin.kdb.utils.StringUtils;

public class Printer {

    public static void main(String[] args) {
        // 定义表格的 header 和 rows
        String[] headers = {"ID", "Name", "Age"};
        Object[][] rows = {
                {1, "Alice", 25},
                {2, "Bob", 30},
                {3, "Charlie", 22}
        };

        // 输出 header
        printRow(headers, true);

        // 输出分割线
        printSeparator(headers.length);

        // 输出 rows
        for (Object[] row : rows) {
            printRow(row, false);
        }
    }

    private static void printRow(Object[] row, boolean isHeader) {
        // 计算每列的最大宽度
        int idWidth = 3; // ID 列固定宽度
        int nameWidth = 10; // 名字列宽度
        int ageWidth = 4; // 年龄列宽度

        // 格式化输出
        String format = "%-" + idWidth + "d | %-" + nameWidth + "s | %-" + ageWidth + "d";
        System.out.format(format, row[0], row[1], row[2]);

        // 如果是 header，则高亮显示
        if (isHeader) {
            System.out.println(" (Header)");
        } else {
            System.out.println();
        }
    }

    private static void printSeparator(int columnCount) {
        // 输出分割线
        StringBuilder separator = new StringBuilder();
        for (int i = 0; i < columnCount; i++) {
            separator.append("+");
            separator.append(StringUtils.repeat("-", 10)); // 假设每列宽度都是10
        }
        separator.append("+");
        System.out.println(separator.toString());
    }
}
