package cn.edu.thssdb.query;

import javafx.util.Pair;
import org.naivedb.Database.Database;
import org.naivedb.Table.*;
import org.naivedb.utils.NDException;
import org.naivedb.Type.*;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.*;

public class RangeVariable {

    private boolean isProduct;
    private boolean isNatural;
    private boolean isOuterJoined;
    private boolean isInnerJoined;
    private boolean isLeft;
    private boolean isRight;
    private boolean isFull;
    private Conditions conditions;
    private ArrayList<RangeVariable> rangeVariables;
    private Table table;
    private String tableName;
    private ArrayList<Table> tableList;
    private ArrayList<ArrayList<Long>> rowNumLists;
    private ArrayList<String> ignoredColumns;

    public RangeVariable(String tableName, Conditions conditions) {
        isProduct = false;
        isOuterJoined = false;
        isInnerJoined = false;
        isLeft = false;
        isRight = false;
        isFull = false;
        rangeVariables = null;
        tableList = null;
        rowNumLists = null;
        ignoredColumns = null;
        table = null;
        this.tableName = tableName;
        this.conditions = conditions;
    }

    public void setProduct(boolean product) {
        isProduct = product;
    }

    public boolean isProduct() {
        return isProduct;
    }

    public void setNatural(boolean natural) {
        isNatural = natural;
    }

    public boolean isNatural() {
        return isNatural;
    }

    public void setOuterJoined(boolean outerJoined) {
        isOuterJoined = outerJoined;
    }

    public boolean isOuterJoined() {
        return isOuterJoined;
    }

    public void setInnerJoined(boolean innerJoined) {
        isInnerJoined = innerJoined;
    }

    public boolean isInnerJoined() {
        return isInnerJoined;
    }

    public void setLeft(boolean left) {
        isLeft = left;
    }

    public boolean isLeft() {
        return isLeft;
    }

    public void setRight(boolean right) {
        isRight = right;
    }

    public boolean isRight() {
        return isRight;
    }

    public void setFull(boolean full) {
        isFull = full;
    }

    public boolean isFull() {
        return isFull;
    }

    public void setConditions(Conditions conditions) {
        this.conditions = conditions;
    }

    public Conditions getConditions() {
        return conditions;
    }

    public void setRowNumLists(ArrayList<ArrayList<Long>> rowNumLists) {
        this.rowNumLists = rowNumLists;
    }

    public void setRangeVariables(ArrayList<RangeVariable> rangeVariables) {
        this.rangeVariables = rangeVariables;
    }

    public ArrayList<ArrayList<Long>> getRowNumLists() {
        return rowNumLists;
    }

    public ArrayList<String> getIgnoredColumns() {
        return ignoredColumns;
    }

    public ArrayList<Table> getTableList() {
        return tableList;
    }

    /**
     * return a temporary table as a result of joining
     */
    public Object exec(Database db) throws NDException, IOException {
        if (rangeVariables == null) {
            return db.getTable(this.tableName);
        }


        combineRows(db);

        ArrayList<String> totalColNames = new ArrayList<>();
        ArrayList<Type> totalColType = new ArrayList<>();

        getTotalColNamesAndTypes(totalColNames, totalColType, tableList);

        ArrayList<Pair<String, Type>> cols = new ArrayList<>();
        for (int i = 0; i < totalColNames.size(); ++i) {
            String colName = totalColNames.get(i);
            Type colType = totalColType.get(i);
            cols.add(new Pair<>(colName, colType));
        }

        TempTable tempTable = new TempTable(cols);

        for (ArrayList<Long> rowNumList : rowNumLists) {

            LinkedList value = new LinkedList(getRowValue(tableList, rowNumList));
            tempTable.insert(value);
        }

        return tempTable;
    }

    /**
     * as we use row number as the middle result of joining operation, this function is to combine the row lists of
     * each table together
     */
    private void combineRows(Database db) throws NDException, IOException {

        if (rangeVariables == null) {
            table = db.getTable(tableName);
            ArrayList<Long> rowNumList = table.getAllRows();
            tableList = new ArrayList<>();
            tableList.add(table);

            rowNumLists = new ArrayList<>();
            for (long rowNum : rowNumList) {
                ArrayList<Long> newNumList = new ArrayList<>();
                newNumList.add(rowNum);
                rowNumLists.add(newNumList);
            }
            return;
        }

        RangeVariable leftRange = rangeVariables.get(0);
        leftRange.combineRows(db);

        if (leftRange.getIgnoredColumns() != null) {
            if (ignoredColumns == null) {
                ignoredColumns = leftRange.getIgnoredColumns();
            } else {
                ignoredColumns.addAll(leftRange.getIgnoredColumns());
            }
        }

        ArrayList<Table> leftTableList = leftRange.getTableList();
        ArrayList<ArrayList<Long>> leftRowNumLists = leftRange.getRowNumLists();

        ArrayList<String> totalColNames = new ArrayList<>();
        ArrayList<Type> totalColTypes = new ArrayList<>();

        getTotalColNamesAndTypes(totalColNames, totalColTypes, leftTableList);

        for (int i = 1; i < rangeVariables.size(); ++i) {
            RangeVariable rightRange = rangeVariables.get(i);
            rightRange.combineRows(db);

            if (rightRange.isNatural()) {
                processNatural(totalColNames, rightRange);
            }

            if (rightRange.isProduct()) {
                getTotalColNamesAndTypes(totalColNames, totalColTypes, rightRange.getTableList());
                product(leftRowNumLists, leftTableList, rightRange);
            }
            else if (rightRange.isOuterJoined()) {
                if (rightRange.isLeft()) {
                    leftOuterJoin(leftRowNumLists, leftTableList, totalColNames, totalColTypes, rightRange);
                }
                else if (rightRange.isRight()) {
                    rightOuterJoin(leftRowNumLists, leftTableList, totalColNames, totalColTypes, rightRange);
                }
                else {
                    fullOuterJoin(leftRowNumLists, leftTableList, totalColNames, totalColTypes, rightRange);
                }
            }
            else {
                innerJoin(leftRowNumLists, leftTableList, totalColNames, totalColTypes, rightRange);
            }
            if (rightRange.getIgnoredColumns() != null) {
                if (ignoredColumns == null) {
                    ignoredColumns = new ArrayList<>();
                }
                ignoredColumns.addAll(rightRange.getIgnoredColumns());
            }
        }

        rowNumLists = leftRowNumLists;
        tableList = leftTableList;
    }

    /**
     * add the total column names and types from a table list to the existed column name list and type list
     */
    private void getTotalColNamesAndTypes(ArrayList<String> totalColNames,
                                          ArrayList<Type> totalColTypes,
                                          ArrayList<Table> tableList) {
        for (Table table : tableList) {
            ArrayList<String> colNames = table.getColNames();
            ArrayList<Type> colTypes = table.getColTypes();
            for (int i = 0; i < colNames.size(); ++i) {
                totalColNames.add(table.getTableName() + "." + colNames.get(i));
                totalColTypes.add(colTypes.get(i));
            }
        }
    }

    /**
     * return a list of actual row value according to the row numbers and tables
     */
    private LinkedList getRowValue(ArrayList<Table> tableList,
                                  ArrayList<Long> rowNumList)
            throws NDException, IOException {
        LinkedList rowValue = new LinkedList();
        for (int i = 0; i < tableList.size(); ++i) {
            long rowNum = rowNumList.get(i);
            Table table = tableList.get(i);
            if (rowNum == -1) {
                for (int j = 0; j < table.getColNames().size(); ++j) {
                    rowValue.add(null);
                }
            } else {
                rowValue.addAll(table.getSingleRowData(rowNum));
            }
        }
        return rowValue;
    }

    private LinkedList getSpecificColValue(ArrayList<Table> tableList,
                                           ArrayList<Long> rowNumList,
                                           ArrayList<String> totalColNameList,
                                           ArrayList<String> specificColNameList)
            throws NDException, IOException{
        ArrayList<Integer> colIndexList = new ArrayList<>();
        int i = 0, j = 0;
        while (i < totalColNameList.size() && j < specificColNameList.size()) {
            if (totalColNameList.get(i).equals(specificColNameList.get(j))) {
                colIndexList.add(i);
                ++i;
                ++j;
            } else {
                ++i;
            }
        }
        LinkedList totalRowValue = getRowValue(tableList, rowNumList);
        LinkedList specificColValue = new LinkedList();
        ListIterator iter = totalRowValue.listIterator();
        i = 0;
        while (iter.hasNext() && i < colIndexList.size()) {
            if (iter.nextIndex() == colIndexList.get(i)) {
                specificColValue.add(iter.next());
                ++i;
            } else {
                iter.next();
            }
        }
        return specificColValue;
    }

    private void product(ArrayList<ArrayList<Long>> leftRowNumLists,
                         ArrayList<Table> leftTableList,
                         RangeVariable rightRange)
            throws NDException, IOException {
        ArrayList<ArrayList<Long>> rightRowNumLists = rightRange.getRowNumLists();
        ArrayList<Table> rightTableList = rightRange.getTableList();

        leftTableList.addAll(rightTableList);

        if (leftRowNumLists.size() == 0 || rightRowNumLists.size() == 0) {
            leftRowNumLists.clear();
            return;
        }

        ArrayList<ArrayList<Long>> tempRowNumLists = new ArrayList<>();

        for (ArrayList<Long> leftRowNumList : leftRowNumLists) {
            for (ArrayList<Long> rightRowNumList : rightRowNumLists) {
                ArrayList<Long> tempRowNumList = new ArrayList<>(leftRowNumList);
                tempRowNumList.addAll(rightRowNumList);
                tempRowNumLists.add(tempRowNumList);
            }
        }

        leftRowNumLists.clear();
        leftRowNumLists.addAll(tempRowNumLists);
    }

    private void leftOuterJoin(ArrayList<ArrayList<Long>> leftRowNumLists,
                               ArrayList<Table> leftTableList,
                               ArrayList<String> leftColNames,
                               ArrayList<Type> leftColTypes,
                               RangeVariable rightRange)
            throws NDException, IOException {

        if (leftRowNumLists.size() == 0) {
            getTotalColNamesAndTypes(leftColNames, leftColTypes, rightRange.getTableList());
            leftTableList.addAll(rightRange.getTableList());
            return;
        }

        ArrayList<ArrayList<Long>> rightRowNumLists = rightRange.getRowNumLists();
        ArrayList<Table> rightTableList = rightRange.getTableList();

        ArrayList<String> rightColNames = new ArrayList<>();
        ArrayList<Type> rightColTypes = new ArrayList<>();
        getTotalColNamesAndTypes(rightColNames, rightColTypes, rightTableList);

        ArrayList<Table> totalTableList = new ArrayList<>(leftTableList);
        totalTableList.addAll(rightTableList);

        rightRange.getConditions().normalize(totalTableList);

        ArrayList<ArrayList<Long>> tempRowNumLists = new ArrayList<>();

        /**
         * single table condition optimization;
         * filter the former table with condition and then do Cartesian product with the latter table
         */
        if (rightRange.getConditions().onlySingleTable(leftTableList)) {
            /** when the single table condition works on the left table */
            LinkedList<String> linkedLeftColNames = new LinkedList<>(leftColNames);
            LinkedList<Type> linkedLeftColTypes = new LinkedList<>(leftColTypes);

            for (ArrayList<Long> leftRowNumList : leftRowNumLists) {
                boolean inserted = false;
                LinkedList leftValue = getRowValue(leftTableList, leftRowNumList);
                if (rightRange.getConditions().satisfied(linkedLeftColNames, linkedLeftColTypes, leftValue)) {
                    inserted = true;
                    for (ArrayList<Long> rightRowNumList : rightRowNumLists) {
                        ArrayList<Long> tempRowNumList = new ArrayList<>(leftRowNumList);
                        tempRowNumList.addAll(rightRowNumList);
                        tempRowNumLists.add(tempRowNumList);
                    }
                }
                /**
                 * implement outer join
                 */
                if (!inserted) {
                    ArrayList<Long> tempRowNumList = new ArrayList<>(leftRowNumList);
                    for (int i = 0; i < rightTableList.size(); ++i) {
                        tempRowNumList.add((long)-1);
                    }
                    tempRowNumLists.add(tempRowNumList);
                }
            }

            leftTableList.addAll(rightTableList);
            getTotalColNamesAndTypes(leftColNames, leftColTypes, rightTableList);

        } else if (rightRange.getConditions().onlySingleTable(rightTableList)) {
            /** when the single table condition works on the right table */
            if (rightRowNumLists.size() == 0) {
                for (ArrayList<Long> leftRowNumList : leftRowNumLists) {
                    ArrayList<Long> tempRowNumList = new ArrayList<>(leftRowNumList);
                    for (int i = 0; i < rightTableList.size(); ++i) {
                        tempRowNumList.add((long)-1);
                    }
                    tempRowNumLists.add(tempRowNumList);
                }
            }
            /**
             * once there is a row from the right table satisfy the condition
             * then all row from left table will be added to the result
             */
            else {
                LinkedList<String> linkedRightColNames = new LinkedList<>(rightColNames);
                LinkedList<Type> linkedRightColTypes = new LinkedList<>(rightColTypes);

                for (ArrayList<Long> rightRowNumList : rightRowNumLists) {
                    LinkedList rightValue = getRowValue(rightTableList, rightRowNumList);
                    if (rightRange.getConditions().satisfied(linkedRightColNames, linkedRightColTypes, rightValue)) {
                        for (ArrayList<Long> leftRowNumList : leftRowNumLists) {
                            ArrayList<Long> tempRowNumList = new ArrayList<>(leftRowNumList);
                            tempRowNumList.addAll(rightRowNumList);
                            tempRowNumLists.add(tempRowNumList);
                        }
                    }
                }
            }

            leftTableList.addAll(rightTableList);
            getTotalColNamesAndTypes(leftColNames, leftColTypes, rightTableList);

        }
        /**
         * hash join optimization
         */
        else if (rightRange.getConditions().twoTablesEqual(leftTableList, rightTableList)) {
            Pair<ArrayList<String>, ArrayList<String>> equalCols = rightRange.getConditions().getTwoTableColumns(leftTableList, rightTableList);
            HashMap<LinkedList, ArrayList<ArrayList<Long>>> index = new HashMap<>();
            for (ArrayList<Long> rightRowNumList : rightRowNumLists) {
                ArrayList<String> rightEqualCols = equalCols.getValue();
                LinkedList rightSpecificValue = getSpecificColValue(rightTableList, rightRowNumList, rightColNames, rightEqualCols);
                if (!index.containsKey(rightSpecificValue)) {
                    ArrayList<ArrayList<Long>> rowNumLists = new ArrayList<>();
                    rowNumLists.add(rightRowNumList);
                    index.put(rightSpecificValue, rowNumLists);
                } else {
                    ArrayList<ArrayList<Long>> rowNumLists = index.get(rightSpecificValue);
                    rowNumLists.add(rightRowNumList);
                }
            }

            for (ArrayList<Long> leftRowNumList : leftRowNumLists) {
                boolean inserted = false;
                ArrayList<String> leftEqualCols = equalCols.getKey();
                LinkedList leftSpecificValue = getSpecificColValue(leftTableList, leftRowNumList, leftColNames, leftEqualCols);
                if (index.containsKey(leftSpecificValue)) {
                    inserted = true;
                    ArrayList<ArrayList<Long>> rowNumLists = index.get(leftSpecificValue);
                    for (ArrayList<Long> rowNumList : rowNumLists) {
                        ArrayList<Long> tempRowNumList = new ArrayList<>(leftRowNumList);
                        tempRowNumList.addAll(rowNumList);
                        tempRowNumLists.add(tempRowNumList);
                    }
                }
                if (!inserted) {
                    ArrayList<Long> tempRowNumList = new ArrayList<>(leftRowNumList);
                    for (int i = 0; i < rightTableList.size(); ++i) {
                        tempRowNumList.add((long)-1);
                    }
                    tempRowNumLists.add(tempRowNumList);
                }
            }

            leftTableList.addAll(rightTableList);
            getTotalColNamesAndTypes(leftColNames, leftColTypes, rightTableList);

        }
        /**
         * nested loop join
         */
        else {

            ArrayList<String> totalColNames = leftColNames;
            ArrayList<Type> totalColTypes = leftColTypes;
            getTotalColNamesAndTypes(totalColNames, totalColTypes, rightTableList);

            LinkedList<String> linkedTotalColNames = new LinkedList<>(totalColNames);
            LinkedList<Type> linkedTotalColTypes = new LinkedList<>(totalColTypes);

            for (ArrayList<Long> leftRowNumList : leftRowNumLists) {
                boolean inserted = false;

                for (ArrayList<Long> rightRowNumList : rightRowNumLists) {
                    ArrayList<Long> tempRowNumList = new ArrayList<>(leftRowNumList);
                    tempRowNumList.addAll(rightRowNumList);
                    LinkedList totalValues = getRowValue(totalTableList, tempRowNumList);
                    if (rightRange.getConditions().satisfied(linkedTotalColNames, linkedTotalColTypes, totalValues)) {
                        tempRowNumLists.add(tempRowNumList);
                        inserted = true;
                    }
                }

                if (!inserted) {
                    ArrayList<Long> tempRowNumList = new ArrayList<>(leftRowNumList);
                    for (int i = 0; i < rightTableList.size(); ++i) {
                        tempRowNumList.add((long)-1);
                    }
                    tempRowNumLists.add(tempRowNumList);
                }
            }

            leftTableList.addAll(rightTableList);
        }

        leftRowNumLists.clear();
        leftRowNumLists.addAll(tempRowNumLists);
    }

    private void rightOuterJoin(ArrayList<ArrayList<Long>> leftRowNumLists,
                               ArrayList<Table> leftTableList,
                               ArrayList<String> leftColNames,
                               ArrayList<Type> leftColTypes,
                               RangeVariable rightRange)
            throws NDException, IOException {

        if (rightRange.getRowNumLists().size() == 0) {
            getTotalColNamesAndTypes(leftColNames, leftColTypes, rightRange.getTableList());
            leftTableList.addAll(rightRange.getTableList());
            leftRowNumLists.clear();
            return;
        }

        ArrayList<ArrayList<Long>> rightRowNumLists = rightRange.getRowNumLists();
        ArrayList<Table> rightTableList = rightRange.getTableList();

        ArrayList<String> rightColNames = new ArrayList<>();
        ArrayList<Type> rightColTypes = new ArrayList<>();
        getTotalColNamesAndTypes(rightColNames, rightColTypes, rightTableList);

        ArrayList<Table> totalTableList = new ArrayList<>(leftTableList);
        totalTableList.addAll(rightTableList);

        rightRange.getConditions().normalize(totalTableList);

        ArrayList<ArrayList<Long>> tempRowNumLists = new ArrayList<>();

        if (rightRange.getConditions().onlySingleTable(leftTableList)) {
            if (leftRowNumLists.size() == 0) {
                for (ArrayList<Long> rightRowNumList : rightRowNumLists) {
                    ArrayList<Long> tempRowNumList = new ArrayList<>();
                    for (int i = 0; i < leftTableList.size(); ++i) {
                        tempRowNumList.add((long)-1);
                    }
                    tempRowNumList.addAll(rightRowNumList);
                    tempRowNumLists.add(tempRowNumList);
                }
            } else {
                LinkedList<String> linkedLeftColNames = new LinkedList<>(leftColNames);
                LinkedList<Type> linkedLeftColTypes = new LinkedList<>(leftColTypes);

                for (ArrayList<Long> leftRowNumList : leftRowNumLists) {
                    LinkedList leftValue = getRowValue(leftTableList, leftRowNumList);
                    if (rightRange.getConditions().satisfied(linkedLeftColNames, linkedLeftColTypes, leftValue)) {
                        for (ArrayList<Long> rightRowNumList : rightRowNumLists) {
                            ArrayList<Long> tempRowNumList = new ArrayList<>(leftRowNumList);
                            tempRowNumList.addAll(rightRowNumList);
                            tempRowNumLists.add(tempRowNumList);
                        }
                    }
                }
            }

            getTotalColNamesAndTypes(leftColNames, leftColTypes, rightTableList);
            leftTableList.addAll(rightTableList);

        } else if (rightRange.getConditions().onlySingleTable(rightTableList)) {
            LinkedList<String> linkedRightColNames = new LinkedList<>(rightColNames);
            LinkedList<Type> linkedLeftColTypes = new LinkedList<>(rightColTypes);

            for (ArrayList<Long> rightRowNumList : rightRowNumLists) {
                boolean inserted = false;
                LinkedList rightValue = getRowValue(rightTableList, rightRowNumList);
                if (rightRange.getConditions().satisfied(linkedRightColNames, linkedLeftColTypes, rightValue)) {
                    for (ArrayList<Long> leftRowNumList : leftRowNumLists) {
                        inserted = true;
                        ArrayList<Long> tempRowNumList = new ArrayList<>(leftRowNumList);
                        tempRowNumList.addAll(rightRowNumList);
                        tempRowNumLists.add(tempRowNumList);
                    }
                }

                if (!inserted) {
                    ArrayList<Long> tempRowNumList = new ArrayList<>();
                    for (int i = 0; i < leftTableList.size(); ++i) {
                        tempRowNumList.add((long)-1);
                    }
                    tempRowNumList.addAll(rightRowNumList);
                    tempRowNumLists.add(tempRowNumList);
                }
            }

            leftTableList.addAll(rightTableList);
            getTotalColNamesAndTypes(leftColNames, leftColTypes, rightTableList);

        } else if (rightRange.getConditions().twoTablesEqual(leftTableList, rightTableList)) {
            Pair<ArrayList<String>, ArrayList<String>> equalCols = rightRange.getConditions().getTwoTableColumns(leftTableList, rightTableList);
            HashMap<LinkedList, ArrayList<ArrayList<Long>>> index = new HashMap<>();
            for (ArrayList<Long> leftRowNumList : leftRowNumLists) {
                ArrayList<String> leftEqualCols = equalCols.getKey();
                LinkedList leftSpecificValue = getSpecificColValue(leftTableList, leftRowNumList, leftColNames, leftEqualCols);
                if (!index.containsKey(leftSpecificValue)) {
                    ArrayList<ArrayList<Long>> rowNumLists = new ArrayList<>();
                    rowNumLists.add(leftRowNumList);
                    index.put(leftSpecificValue, rowNumLists);
                } else {
                    ArrayList<ArrayList<Long>> rowNumLists = index.get(leftSpecificValue);
                    rowNumLists.add(leftRowNumList);
                }
            }

            for (ArrayList<Long> rightRowNumList : rightRowNumLists) {
                boolean inserted = false;
                ArrayList<String> rightEqualCols = equalCols.getValue();
                LinkedList rightSpecificValue = getSpecificColValue(rightTableList, rightRowNumList, rightColNames, rightEqualCols);
                if (index.containsKey(rightSpecificValue)) {
                    inserted = true;
                    ArrayList<ArrayList<Long>> rowNumLists = index.get(rightSpecificValue);
                    for (ArrayList<Long> rowNumList : rowNumLists) {
                        ArrayList<Long> tempRowNumList = new ArrayList<>(rowNumList);
                        tempRowNumList.addAll(rightRowNumList);
                        tempRowNumLists.add(tempRowNumList);
                    }
                }
                if (!inserted) {
                    ArrayList<Long> tempRowNumList = new ArrayList<>();
                    for (int i = 0; i < leftTableList.size(); ++i) {
                        tempRowNumList.add((long)-1);
                    }
                    tempRowNumList.addAll(rightRowNumList);
                    tempRowNumLists.add(tempRowNumList);
                }
            }

            leftTableList.addAll(rightTableList);
            getTotalColNamesAndTypes(leftColNames, leftColTypes, rightTableList);

        } else {

            ArrayList<String> totalColNames = leftColNames;
            ArrayList<Type> totalColTypes = leftColTypes;
            getTotalColNamesAndTypes(totalColNames, totalColTypes, rightTableList);

            LinkedList<String> linkedTotalColNames = new LinkedList<>(totalColNames);
            LinkedList<Type> linkedTotalColTypes = new LinkedList<>(totalColTypes);

            for (ArrayList<Long> rightRowNumList : rightRowNumLists) {
                boolean inserted = false;

                for (ArrayList<Long> leftRowNumList : leftRowNumLists) {
                    ArrayList<Long> tempRowNumList = new ArrayList<>(leftRowNumList);
                    tempRowNumList.addAll(rightRowNumList);
                    LinkedList totalValues = getRowValue(totalTableList, tempRowNumList);
                    if (rightRange.getConditions().satisfied(linkedTotalColNames, linkedTotalColTypes, totalValues)) {
                        tempRowNumLists.add(tempRowNumList);
                        inserted = true;
                    }
                }

                if (!inserted) {
                    ArrayList<Long> tempRowNumList = new ArrayList<>();
                    for (int i = 0; i < leftTableList.size(); ++i) {
                        tempRowNumList.add((long)-1);
                    }
                    tempRowNumList.addAll(rightRowNumList);
                    tempRowNumLists.add(tempRowNumList);
                }

                leftTableList.addAll(rightTableList);
            }
        }

        leftRowNumLists.clear();
        leftRowNumLists.addAll(tempRowNumLists);
    }


    private void fullOuterJoin(ArrayList<ArrayList<Long>> leftRowNumLists,
                                ArrayList<Table> leftTableList,
                                ArrayList<String> leftColNames,
                                ArrayList<Type> leftColTypes,
                                RangeVariable rightRange)
            throws NDException, IOException {
        ArrayList<ArrayList<Long>> rightRowNumLists = rightRange.getRowNumLists();
        ArrayList<Table> rightTableList = rightRange.getTableList();

        if (leftRowNumLists.size() == 0 && rightRowNumLists.size() == 0) {
            getTotalColNamesAndTypes(leftColNames, leftColTypes, rightTableList);
            leftTableList.addAll(rightTableList);
            return;
        }

        ArrayList<String> rightColNames = new ArrayList<>();
        ArrayList<Type> rightColTypes = new ArrayList<>();
        getTotalColNamesAndTypes(rightColNames, rightColTypes, rightTableList);

        ArrayList<Table> totalTableList = new ArrayList<>(leftTableList);
        totalTableList.addAll(rightTableList);

        rightRange.getConditions().normalize(totalTableList);

        ArrayList<ArrayList<Long>> tempRowNumLists = new ArrayList<>();

        if (rightRange.getConditions().onlySingleTable(leftTableList)) {
            leftOuterJoin(leftRowNumLists, leftTableList, leftColNames, leftColTypes, rightRange);
            if (rightRowNumLists.size() != 0 && leftRowNumLists.size() == 0) {
                for (ArrayList<Long> rightRowNumList : rightRowNumLists) {
                    ArrayList<Long> tempRowNumList = new ArrayList<>();
                    for (int i = 0; i < leftTableList.size() - rightTableList.size(); ++i) {
                        tempRowNumList.add((long)-1);
                    }
                    tempRowNumList.addAll(rightRowNumList);
                    leftRowNumLists.add(tempRowNumList);
                }
            }
            return;

        } else if (rightRange.getConditions().onlySingleTable(rightTableList)) {
            ArrayList<ArrayList<Long>> oldLeftRowNumLists = new ArrayList<>(leftRowNumLists);
            rightOuterJoin(leftRowNumLists, leftTableList, leftColNames, leftColTypes, rightRange);
            if (oldLeftRowNumLists.size() != 0 && leftRowNumLists.size() == 0) {
                for (ArrayList<Long> leftRowNumList : oldLeftRowNumLists) {
                    ArrayList<Long> tempRowNumList = new ArrayList<>(leftRowNumList);
                    for (int i = 0; i < rightTableList.size(); ++i) {
                        tempRowNumList.add((long)-1);
                    }
                    leftRowNumLists.add(tempRowNumList);
                }
            }
            return;

        } else if (rightRange.getConditions().twoTablesEqual(leftTableList, rightTableList)) {
            Pair<ArrayList<String>, ArrayList<String>> equalCols = rightRange.getConditions().getTwoTableColumns(leftTableList, rightTableList);
            HashMap<LinkedList, ArrayList<ArrayList<Long>>> index = new HashMap<>();
            for (ArrayList<Long> leftRowNumList : leftRowNumLists) {
                ArrayList<String> leftEqualCols = equalCols.getKey();
                LinkedList leftSpecificValue = getSpecificColValue(leftTableList, leftRowNumList, leftColNames, leftEqualCols);
                if (!index.containsKey(leftSpecificValue)) {
                    ArrayList<ArrayList<Long>> rowNumLists = new ArrayList<>();
                    rowNumLists.add(leftRowNumList);
                    index.put(leftSpecificValue, rowNumLists);
                } else {
                    ArrayList<ArrayList<Long>> rowNumLists = index.get(leftSpecificValue);
                    rowNumLists.add(leftRowNumList);
                }
            }

            HashSet<ArrayList<Long>> leftRowNumSet = new HashSet<>();
            for (ArrayList<Long> rightRowNumList : rightRowNumLists) {
                boolean inserted = false;
                ArrayList<String> rightEqualCols = equalCols.getValue();
                LinkedList rightSpecificValue = getSpecificColValue(rightTableList, rightRowNumList, rightColNames, rightEqualCols);
                if (index.containsKey(rightSpecificValue)) {
                    inserted = true;
                    ArrayList<ArrayList<Long>> rowNumLists = index.get(rightSpecificValue);
                    for (ArrayList<Long> rowNumList : rowNumLists) {
                        leftRowNumSet.add(rowNumList);
                        ArrayList<Long> tempRowNumList = new ArrayList<>(rowNumList);
                        tempRowNumList.addAll(rightRowNumList);
                        tempRowNumLists.add(tempRowNumList);
                    }
                }
                if (!inserted) {
                    ArrayList<Long> tempRowNumList = new ArrayList<>();
                    for (int i = 0; i < leftTableList.size(); ++i) {
                        tempRowNumList.add((long)-1);
                    }
                    tempRowNumList.addAll(rightRowNumList);
                    tempRowNumLists.add(tempRowNumList);
                }
            }

            for (ArrayList<Long> leftRowNumList : leftRowNumLists) {
                if (!leftRowNumSet.contains(leftRowNumList)) {
                    ArrayList<Long> tempRowNumList = new ArrayList<>(leftRowNumList);
                    for (int i = 0; i < rightTableList.size(); ++i) {
                        tempRowNumList.add((long)-1);
                    }
                    tempRowNumLists.add(tempRowNumList);
                }
            }

            leftTableList.addAll(rightTableList);
            getTotalColNamesAndTypes(leftColNames, leftColTypes, rightTableList);

        } else {

            ArrayList<String> totalColNames = leftColNames;
            ArrayList<Type> totalColTypes = leftColTypes;
            getTotalColNamesAndTypes(leftColNames, leftColTypes, rightTableList);

            LinkedList<String> linkedTotalColNames = new LinkedList<>(totalColNames);
            LinkedList<Type> linkedTotalColTypes = new LinkedList<>(totalColTypes);

            HashSet<ArrayList<Long>> rightRowNumSet = new HashSet<>();

            for (ArrayList<Long> leftRowNumList : leftRowNumLists) {
                boolean inserted = false;

                for (ArrayList<Long> rightRowNumList : rightRowNumLists) {
                    ArrayList<Long> tempRowNumList = new ArrayList<>(leftRowNumList);
                    tempRowNumList.addAll(rightRowNumList);
                    LinkedList totalValues = getRowValue(totalTableList, tempRowNumList);
                    if (rightRange.getConditions().satisfied(linkedTotalColNames, linkedTotalColTypes, totalValues)) {
                        tempRowNumLists.add(tempRowNumList);
                        inserted = true;
                        rightRowNumSet.add(rightRowNumList);
                    }
                }

                if (!inserted) {
                    ArrayList<Long> tempRowNumList = new ArrayList<>(leftRowNumList);
                    for (int i = 0; i < rightTableList.size(); ++i) {
                        tempRowNumList.add((long)-1);
                    }
                    tempRowNumLists.add(tempRowNumList);
                }
            }

            for (ArrayList<Long> rightRowNumList : rightRowNumLists) {
                if (!rightRowNumSet.contains(rightRowNumList)) {
                    ArrayList<Long> tempRowNumList = new ArrayList<>();
                    for (int i = 0; i < leftTableList.size(); ++i) {
                        tempRowNumList.add((long)-1);
                    }
                    tempRowNumList.addAll(rightRowNumList);
                    tempRowNumLists.add(tempRowNumList);
                }
            }

            leftTableList.addAll(rightTableList);
        }

        leftRowNumLists.clear();
        leftRowNumLists.addAll(tempRowNumLists);
    }
    private void innerJoin(ArrayList<ArrayList<Long>> leftRowNumLists,
                                ArrayList<Table> leftTableList,
                                ArrayList<String> leftColNames,
                                ArrayList<Type> leftColTypes,
                                RangeVariable rightRange)
            throws NDException, IOException {

        if (leftRowNumLists.size() == 0) {
            getTotalColNamesAndTypes(leftColNames, leftColTypes, rightRange.getTableList());
            leftTableList.addAll(rightRange.getTableList());
            return;
        }

        ArrayList<ArrayList<Long>> rightRowNumLists = rightRange.getRowNumLists();
        ArrayList<Table> rightTableList = rightRange.getTableList();

        ArrayList<String> rightColNames = new ArrayList<>();
        ArrayList<Type> rightColTypes = new ArrayList<>();
        getTotalColNamesAndTypes(rightColNames, rightColTypes, rightTableList);

        ArrayList<Table> totalTableList = new ArrayList<>(leftTableList);
        totalTableList.addAll(rightTableList);

        rightRange.getConditions().normalize(totalTableList);

        ArrayList<ArrayList<Long>> tempRowNumLists = new ArrayList<>();

        if (rightRange.getConditions().onlySingleTable(leftTableList)) {
            LinkedList<String> linkedLeftColNames = new LinkedList<>(leftColNames);
            LinkedList<Type> linkedLeftColTypes = new LinkedList<>(leftColTypes);

            for (ArrayList<Long> leftRowNumList : leftRowNumLists) {
                LinkedList leftValue = getRowValue(leftTableList, leftRowNumList);
                if (rightRange.getConditions().satisfied(linkedLeftColNames, linkedLeftColTypes, leftValue)) {
                    for (ArrayList<Long> rightRowNumList : rightRowNumLists) {
                        ArrayList<Long> tempRowNumList = new ArrayList<>(leftRowNumList);
                        tempRowNumList.addAll(rightRowNumList);
                        tempRowNumLists.add(tempRowNumList);
                    }
                }
            }

            leftTableList.addAll(rightTableList);
            getTotalColNamesAndTypes(leftColNames, leftColTypes, rightTableList);

        } else if (rightRange.getConditions().onlySingleTable(rightTableList)) {
            LinkedList<String> linkedRightColNames = new LinkedList<>(rightColNames);
            LinkedList<Type> linkedRightColTypes = new LinkedList<>(rightColTypes);

            for (ArrayList<Long> rightRowNumList : rightRowNumLists) {
                LinkedList rightValue = getRowValue(rightTableList, rightRowNumList);
                if (rightRange.getConditions().satisfied(linkedRightColNames, linkedRightColTypes, rightValue)) {
                    for (ArrayList<Long> leftRowNumList : leftRowNumLists) {
                        ArrayList<Long> tempRowNumList = new ArrayList<>(leftRowNumList);
                        tempRowNumList.addAll(rightRowNumList);
                        tempRowNumLists.add(tempRowNumList);
                    }
                }
            }

            leftTableList.addAll(rightTableList);
            getTotalColNamesAndTypes(leftColNames, leftColTypes, rightTableList);

        } else if (rightRange.getConditions().twoTablesEqual(leftTableList, rightTableList)) {
            Pair<ArrayList<String>, ArrayList<String>> equalCols = rightRange.getConditions().getTwoTableColumns(leftTableList, rightTableList);
            HashMap<LinkedList, ArrayList<ArrayList<Long>>> index = new HashMap<>();
            for (ArrayList<Long> leftRowNumList : leftRowNumLists) {
                ArrayList<String> leftEqualCols = equalCols.getKey();
                LinkedList leftSpecificValue = getSpecificColValue(leftTableList, leftRowNumList, leftColNames, leftEqualCols);
                if (!index.containsKey(leftSpecificValue)) {
                    ArrayList<ArrayList<Long>> rowNumLists = new ArrayList<>();
                    rowNumLists.add(leftRowNumList);
                    index.put(leftSpecificValue, rowNumLists);
                } else {
                    ArrayList<ArrayList<Long>> rowNumLists = index.get(leftSpecificValue);
                    rowNumLists.add(leftRowNumList);
                }
            }

            for (ArrayList<Long> rightRowNumList : rightRowNumLists) {
                ArrayList<String> rightEqualCols = equalCols.getValue();
                LinkedList rightSpecificValue = getSpecificColValue(rightTableList, rightRowNumList, rightColNames, rightEqualCols);
                if (index.containsKey(rightSpecificValue)) {
                    ArrayList<ArrayList<Long>> rowNumLists = index.get(rightSpecificValue);
                    for (ArrayList<Long> rowNumList : rowNumLists) {
                        ArrayList<Long> tempRowNumList = new ArrayList<>(rowNumList);
                        tempRowNumList.addAll(rightRowNumList);
                        tempRowNumLists.add(tempRowNumList);
                    }
                }
            }

            leftTableList.addAll(rightTableList);
            getTotalColNamesAndTypes(leftColNames, leftColTypes, rightTableList);

        } else {

            ArrayList<String> totalColNames = leftColNames;
            ArrayList<Type> totalColTypes = leftColTypes;
            getTotalColNamesAndTypes(totalColNames, totalColTypes, rightTableList);

            LinkedList<String> newTotalColNames = new LinkedList<>(totalColNames);
            LinkedList<Type> newTotalColTypes = new LinkedList<>(totalColTypes);

            rightRange.getConditions().normalize(totalTableList);

            for (ArrayList<Long> leftRowNumList : leftRowNumLists) {
                for (ArrayList<Long> rightRowNumList : rightRowNumLists) {
                    ArrayList<Long> tempRowNumList = new ArrayList<>(leftRowNumList);
                    tempRowNumList.addAll(rightRowNumList);
                    LinkedList totalValues = getRowValue(totalTableList, tempRowNumList);
                    if (rightRange.getConditions().satisfied(newTotalColNames, newTotalColTypes, totalValues)) {
                        tempRowNumLists.add(tempRowNumList);
                    }
                }
            }

            leftTableList.addAll(rightTableList);
        }

        leftRowNumLists.clear();
        leftRowNumLists.addAll(tempRowNumLists);
    }

    /**
     * find the columns with a same name, and build a condition,
     * then add the column name to the ignore column list
     */
    private void processNatural(ArrayList<String> leftColNames, RangeVariable rightRange) throws NDException {
        ArrayList<Table> rightTableList = rightRange.getTableList();
        ArrayList<String> rightColNames = new ArrayList<>();
        for (Table rightTable : rightTableList) {
            ArrayList<String> colNames = rightTable.getColNames();
            String tableName = rightTable.getTableName();
            for (String colName : colNames) {
                rightColNames.add(tableName + "." + colName);
            }
        }
        leftColNames = new ArrayList<>(leftColNames);
        if (ignoredColumns != null)
            leftColNames.removeAll(ignoredColumns);
        if (rightRange.getIgnoredColumns() != null)
            rightColNames.removeAll(rightRange.getIgnoredColumns());

        checkDuplicateColName(leftColNames);
        checkDuplicateColName(rightColNames);

        Conditions naturalCond = null;

        for (String leftColName : leftColNames) {
            String exactLeftColName = getExactColName(leftColName);
            for (String rightColName : rightColNames) {
                String exactRightColName = getExactColName(rightColName);
                if (exactLeftColName.equals(exactRightColName)) {
                    Expression leftExpr = new Expression(1, leftColName);
                    Expression rightExpr = new Expression(1, rightColName);
                    Conditions cond = new Conditions(2, "EQ", leftExpr, rightExpr);
                    if (naturalCond == null) {
                        naturalCond = cond;
                    } else {
                        naturalCond = new Conditions(0, naturalCond, cond);
                    }
                    if (ignoredColumns == null){
                        ignoredColumns = new ArrayList<>();
                    }
                    ignoredColumns.add(rightColName);
                }
            }
        }
        if (naturalCond == null) {
            rightRange.setProduct(true);
            rightRange.setOuterJoined(false);
            rightRange.setInnerJoined(false);
        }
        else {
            rightRange.setConditions(naturalCond);
        }
    }

    private String getExactColName(String colName) {
        int sepPos = colName.indexOf('.');
        if (sepPos == -1) {
            return colName;
        }
        return colName.substring(sepPos + 1);
    }

    private void checkDuplicateColName(ArrayList<String> colNameList) throws NDException {
        HashSet<String> colNameSet = new HashSet<>();
        for (String colName : colNameList) {
            String exactColName = getExactColName(colName);
            if (colNameSet.contains(exactColName)) {
                throw new NDException("Duplicate column name: " + "\"" + exactColName + "\"" + " in derived table");
            }
            colNameSet.add(exactColName);
        }
    }
}