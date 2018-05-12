package org.mengyun.tcctransaction.repository;

import org.mengyun.tcctransaction.Transaction;
import org.mengyun.tcctransaction.api.TransactionStatus;
import org.mengyun.tcctransaction.serializer.JdkSerializationSerializer;
import org.mengyun.tcctransaction.serializer.ObjectSerializer;
import org.mengyun.tcctransaction.utils.CollectionUtils;
import org.mengyun.tcctransaction.utils.StringUtils;

import javax.sql.DataSource;
import javax.transaction.xa.Xid;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * JDBC事务存储器,用于通过JDBC驱动将事务存储到MySQL/Oracle/PostgreSQL/SQLServer等关系数据库
 */
public class JdbcTransactionRepository extends CachableTransactionRepository {

    /**
     * 领域,用于唯一标识一个资源
     */
    private String domain;

    /**
     * 表后缀,默认存储表名为 TCC_TRANSACTION,配置表后缀为TCC_TRANSACTION${tbSuffix}
     */
    private String tbSuffix;

    /**
     * 数据源
     */
    private DataSource dataSource;

    /**
     * 序列化
     */
    private ObjectSerializer serializer = new JdkSerializationSerializer();

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getTbSuffix() {
        return tbSuffix;
    }

    public void setTbSuffix(String tbSuffix) {
        this.tbSuffix = tbSuffix;
    }

    public void setSerializer(ObjectSerializer serializer) {
        this.serializer = serializer;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    /**
     * 新增事务
     *
     * @param transaction
     * @return
     */
    protected int doCreate(Transaction transaction) {
        Connection connection = null;
        PreparedStatement stmt = null;

        try {
            connection = this.getConnection();

            StringBuilder builder = new StringBuilder();
            builder.append("INSERT INTO " + getTableName() +
                    "(GLOBAL_TX_ID,BRANCH_QUALIFIER,TRANSACTION_TYPE,CONTENT,STATUS,RETRIED_COUNT,CREATE_TIME,LAST_UPDATE_TIME,VERSION");
            builder.append(StringUtils.isNotEmpty(domain) ? ",DOMAIN ) VALUES (?,?,?,?,?,?,?,?,?,?)" : ") VALUES (?,?,?,?,?,?,?,?,?)");

            stmt = connection.prepareStatement(builder.toString());

            stmt.setBytes(1, transaction.getXid().getGlobalTransactionId());
            stmt.setBytes(2, transaction.getXid().getBranchQualifier());
            stmt.setInt(3, transaction.getTransactionType().getId());
            //序列化事务
            stmt.setBytes(4, serializer.serialize(transaction));
            stmt.setInt(5, transaction.getStatus().getId());
            stmt.setInt(6, transaction.getRetriedCount());
            stmt.setTimestamp(7, new java.sql.Timestamp(transaction.getCreateTime().getTime()));
            stmt.setTimestamp(8, new java.sql.Timestamp(transaction.getLastUpdateTime().getTime()));
            stmt.setLong(9, transaction.getVersion());

            if (StringUtils.isNotEmpty(domain)) {
                stmt.setString(10, domain);
            }

            return stmt.executeUpdate();
        } catch (SQLException e) {
            throw new TransactionIOException(e);
        } finally {
            closeStatement(stmt);
            this.releaseConnection(connection);
        }
    }

    /**
     * 更新事务
     *
     * @param transaction
     * @return
     */
    protected int doUpdate(Transaction transaction) {
        Connection connection = null;
        PreparedStatement stmt = null;

        java.util.Date lastUpdateTime = transaction.getLastUpdateTime();
        long currentVersion = transaction.getVersion();

        //设置事务最后更新时间
        transaction.updateTime();
        //设置事务最新版本号
        transaction.updateVersion();

        try {
            connection = this.getConnection();

            StringBuilder builder = new StringBuilder();
            builder.append("UPDATE " + getTableName() + " SET " +
                    "CONTENT = ?,STATUS = ?,LAST_UPDATE_TIME = ?, RETRIED_COUNT = ?,VERSION = VERSION+1 WHERE GLOBAL_TX_ID = ? AND BRANCH_QUALIFIER = ? AND VERSION = ?");

            builder.append(StringUtils.isNotEmpty(domain) ? " AND DOMAIN = ?" : "");

            stmt = connection.prepareStatement(builder.toString());

            //序列化事务
            stmt.setBytes(1, serializer.serialize(transaction));
            stmt.setInt(2, transaction.getStatus().getId());
            stmt.setTimestamp(3, new Timestamp(transaction.getLastUpdateTime().getTime()));
            stmt.setInt(4, transaction.getRetriedCount());
            stmt.setBytes(5, transaction.getXid().getGlobalTransactionId());
            stmt.setBytes(6, transaction.getXid().getBranchQualifier());
            stmt.setLong(7, currentVersion);

            if (StringUtils.isNotEmpty(domain)) {
                stmt.setString(8, domain);
            }

            int result = stmt.executeUpdate();

            return result;
        } catch (Throwable e) {
            transaction.setLastUpdateTime(lastUpdateTime);
            transaction.setVersion(currentVersion);
            throw new TransactionIOException(e);
        } finally {
            closeStatement(stmt);
            this.releaseConnection(connection);
        }
    }

    /**
     * 删除事务
     *
     * @param transaction
     * @return
     */
    protected int doDelete(Transaction transaction) {
        Connection connection = null;
        PreparedStatement stmt = null;

        try {
            connection = this.getConnection();

            StringBuilder builder = new StringBuilder();
            builder.append("DELETE FROM " + getTableName() +
                    " WHERE GLOBAL_TX_ID = ? AND BRANCH_QUALIFIER = ?");

            builder.append(StringUtils.isNotEmpty(domain) ? " AND DOMAIN = ?" : "");

            stmt = connection.prepareStatement(builder.toString());

            stmt.setBytes(1, transaction.getXid().getGlobalTransactionId());
            stmt.setBytes(2, transaction.getXid().getBranchQualifier());

            if (StringUtils.isNotEmpty(domain)) {
                stmt.setString(3, domain);
            }

            return stmt.executeUpdate();
        } catch (SQLException e) {
            throw new TransactionIOException(e);
        } finally {
            closeStatement(stmt);
            this.releaseConnection(connection);
        }
    }

    /**
     * 根据事务编号查询事务
     *
     * @param xid
     * @return
     */
    protected Transaction doFindOne(Xid xid) {
        List<Transaction> transactions = doFind(Arrays.asList(xid));

        if (!CollectionUtils.isEmpty(transactions)) {
            return transactions.get(0);
        }
        return null;
    }

    /**
     * 获取超过指定时间的事务集合
     *
     * @param date
     * @return
     */
    @Override
    protected List<Transaction> doFindAllUnmodifiedSince(java.util.Date date) {
        List<Transaction> transactions = new ArrayList<Transaction>();

        Connection connection = null;
        PreparedStatement stmt = null;

        try {
            connection = this.getConnection();

            StringBuilder builder = new StringBuilder();
            builder.append("SELECT GLOBAL_TX_ID, BRANCH_QUALIFIER, CONTENT,STATUS,TRANSACTION_TYPE,CREATE_TIME,LAST_UPDATE_TIME,RETRIED_COUNT,VERSION");
            builder.append(StringUtils.isNotEmpty(domain) ? ",DOMAIN" : "");
            builder.append("  FROM " + getTableName() + " WHERE LAST_UPDATE_TIME < ?");
            builder.append(StringUtils.isNotEmpty(domain) ? " AND DOMAIN = ?" : "");

            stmt = connection.prepareStatement(builder.toString());

            stmt.setTimestamp(1, new Timestamp(date.getTime()));

            if (StringUtils.isNotEmpty(domain)) {
                stmt.setString(2, domain);
            }

            ResultSet resultSet = stmt.executeQuery();
            this.constructTransactions(resultSet, transactions);
        } catch (Throwable e) {
            throw new TransactionIOException(e);
        } finally {
            closeStatement(stmt);
            this.releaseConnection(connection);
        }

        return transactions;
    }

    /**
     * 根据事务编号集合查询事务集合
     *
     * @param xids
     * @return
     */
    protected List<Transaction> doFind(List<Xid> xids) {
        List<Transaction> transactions = new ArrayList<Transaction>();

        if (CollectionUtils.isEmpty(xids)) {
            return transactions;
        }

        Connection connection = null;
        PreparedStatement stmt = null;

        try {
            connection = this.getConnection();

            StringBuilder builder = new StringBuilder();
            builder.append("SELECT GLOBAL_TX_ID, BRANCH_QUALIFIER, CONTENT,STATUS,TRANSACTION_TYPE,CREATE_TIME,LAST_UPDATE_TIME,RETRIED_COUNT,VERSION");
            builder.append(StringUtils.isNotEmpty(domain) ? ",DOMAIN" : "");
            builder.append("  FROM " + getTableName() + " WHERE");

            if (!CollectionUtils.isEmpty(xids)) {
                for (Xid xid : xids) {
                    builder.append(" ( GLOBAL_TX_ID = ? AND BRANCH_QUALIFIER = ? ) OR");
                }

                builder.delete(builder.length() - 2, builder.length());
            }

            builder.append(StringUtils.isNotEmpty(domain) ? " AND DOMAIN = ?" : "");

            stmt = connection.prepareStatement(builder.toString());

            int i = 0;

            for (Xid xid : xids) {
                stmt.setBytes(++i, xid.getGlobalTransactionId());
                stmt.setBytes(++i, xid.getBranchQualifier());
            }

            if (StringUtils.isNotEmpty(domain)) {
                stmt.setString(++i, domain);
            }

            ResultSet resultSet = stmt.executeQuery();

            this.constructTransactions(resultSet, transactions);
        } catch (Throwable e) {
            throw new TransactionIOException(e);
        } finally {
            closeStatement(stmt);
            this.releaseConnection(connection);
        }

        return transactions;
    }

    /**
     * 根据结果集构造事务集合
     *
     * @param resultSet
     * @param transactions
     * @throws SQLException
     */
    protected void constructTransactions(ResultSet resultSet, List<Transaction> transactions) throws SQLException {
        while (resultSet.next()) {
            byte[] transactionBytes = resultSet.getBytes(3);
            Transaction transaction = (Transaction) serializer.deserialize(transactionBytes);
            transaction.changeStatus(TransactionStatus.valueOf(resultSet.getInt(4)));
            transaction.setLastUpdateTime(resultSet.getDate(7));
            transaction.setVersion(resultSet.getLong(9));
            transaction.resetRetriedCount(resultSet.getInt(8));
            transactions.add(transaction);
        }
    }

    /**
     * 获取数据源连接
     *
     * @return
     */
    protected Connection getConnection() {
        try {
            return this.dataSource.getConnection();
        } catch (SQLException e) {
            throw new TransactionIOException(e);
        }
    }

    /**
     * 释放数据源连接
     *
     * @param con
     */
    protected void releaseConnection(Connection con) {
        try {
            if (con != null && !con.isClosed()) {
                con.close();
            }
        } catch (SQLException e) {
            throw new TransactionIOException(e);
        }
    }

    /**
     * 释放声明
     *
     * @param stmt
     */
    private void closeStatement(Statement stmt) {
        try {
            if (stmt != null && !stmt.isClosed()) {
                stmt.close();
            }
        } catch (Exception ex) {
            throw new TransactionIOException(ex);
        }
    }

    /**
     * 根据表后缀获取表名
     *
     * @return
     */
    private String getTableName() {
        return StringUtils.isNotEmpty(tbSuffix) ? "TCC_TRANSACTION" + tbSuffix : "TCC_TRANSACTION";
    }
}