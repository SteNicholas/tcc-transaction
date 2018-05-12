package org.mengyun.tcctransaction.server.controller;

import org.apache.commons.lang3.StringUtils;
import org.mengyun.tcctransaction.server.dao.DaoRepository;
import org.mengyun.tcctransaction.server.vo.CommonResponse;
import org.mengyun.tcctransaction.server.vo.TransactionVo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.ModelAndView;

import java.util.List;

/**
 * 事务控制器
 */
@Controller
@RequestMapping("/management")
public class TransactionController {

    @Autowired
    private DaoRepository daoRepository;

    public static final Integer DEFAULT_PAGE_NUM = 1;

    public static final int DEFAULT_PAGE_SIZE = 10;

    /**
     * 分页查看事务列表
     *
     * @param domain
     * @param pageNum
     * @return
     */
    @RequestMapping(method = RequestMethod.GET)
    public ModelAndView manager(@RequestParam(value = "domain", required = false) String domain, @RequestParam(value = "pagenum", required = false) Integer pageNum) {
        if (StringUtils.isEmpty(domain)) {
            return manager();
        }

        if (pageNum == null) {
            return manager(domain);
        }

        ModelAndView modelAndView = new ModelAndView("manager");
        List<TransactionVo> transactionVos = daoRepository.getDao(domain).findTransactions(pageNum, DEFAULT_PAGE_SIZE);
        Integer totalCount = daoRepository.getDao(domain).countOfFindTransactions();
        Integer pages = totalCount / DEFAULT_PAGE_SIZE;
        if (totalCount % DEFAULT_PAGE_SIZE > 0) {
            pages++;
        }
        modelAndView.addObject("transactionVos", transactionVos);
        modelAndView.addObject("pageNum", pageNum);
        modelAndView.addObject("pageSize", DEFAULT_PAGE_SIZE);
        modelAndView.addObject("pages", pages);
        modelAndView.addObject("currentDomain", domain);
        modelAndView.addObject("domains", daoRepository.getDomains());
        modelAndView.addObject("urlWithoutPaging", "management?domain=" + domain);
        return modelAndView;
    }

    /**
     * 重置事务恢复重试次数
     *
     * @param domain
     * @param globalTxId
     * @param branchQualifier
     * @return
     */
    @RequestMapping(value = "/retry/reset", method = RequestMethod.PUT)
    @ResponseBody
    public CommonResponse<Void> reset(String domain, String globalTxId, String branchQualifier) {
        daoRepository.getDao(domain).resetRetryCount(
                globalTxId,
                branchQualifier);

        return new CommonResponse<Void>();
    }

    /**
     * 重置事务恢复重试次数
     *
     * @param domain
     * @param globalTxId
     * @param branchQualifier
     * @return
     */
    @RequestMapping(value = "/retry/delete", method = RequestMethod.PUT)
    @ResponseBody
    public CommonResponse<Void> delete(String domain, String globalTxId, String branchQualifier) {
        daoRepository.getDao(domain).resetRetryCount(
                globalTxId,
                branchQualifier);

        return new CommonResponse<Void>();
    }

    /**
     * 重置事务恢复重试次数
     *
     * @param domain
     * @param globalTxId
     * @param branchQualifier
     * @return
     */
    @RequestMapping(value = "/retry/confirm", method = RequestMethod.PUT)
    @ResponseBody
    public CommonResponse<Void> confirm(String domain, String globalTxId, String branchQualifier) {
        daoRepository.getDao(domain).resetRetryCount(
                globalTxId,
                branchQualifier);

        return new CommonResponse<Void>();
    }

    /**
     * 重置事务恢复重试次数
     *
     * @param domain
     * @param globalTxId
     * @param branchQualifier
     * @return
     */
    @RequestMapping(value = "/retry/cancel", method = RequestMethod.PUT)
    @ResponseBody
    public CommonResponse<Void> cancel(String domain, String globalTxId, String branchQualifier) {
        daoRepository.getDao(domain).resetRetryCount(
                globalTxId,
                branchQualifier);

        return new CommonResponse<Void>();
    }

    public ModelAndView manager() {
        ModelAndView modelAndView = new ModelAndView("manager");
        modelAndView.addObject("domains", daoRepository.getDomains());
        return modelAndView;
    }

    public ModelAndView manager(String domain) {
        return manager(domain, DEFAULT_PAGE_NUM);
    }
}