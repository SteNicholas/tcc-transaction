package org.mengyun.tcctransaction.server.dao;

import org.mengyun.tcctransaction.server.dto.PageDto;
import org.mengyun.tcctransaction.server.vo.TransactionVo;

import java.util.List;

/**
 * 事务Dao
 */
public interface TransactionDao {

    /**
     * 分页获取事务Vo集合
     *
     * @param pageNum
     * @param pageSize
     * @return
     */
    public List<TransactionVo> findTransactions(Integer pageNum, int pageSize);

    /**
     * 获取事务总数量
     *
     * @return
     */
    public Integer countOfFindTransactions();

    /**
     * 重置事务重试次数
     *
     * @param globalTxId
     * @param branchQualifier
     */
    public void resetRetryCount(String globalTxId, String branchQualifier);

    public void delete(String globalTxId, String branchQualifier);

    public void confirm(String globalTxId, String branchQualifier);

    public void cancel(String globalTxId, String branchQualifier);

    public String getDomain();

    public PageDto<TransactionVo> findTransactionPageDto(Integer pageNum, int pageSize);
}