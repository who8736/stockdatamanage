CREATE DATABASE  IF NOT EXISTS `stockdata` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin */ /*!80016 DEFAULT ENCRYPTION='N' */;
USE `stockdata`;
-- MySQL dump 10.13  Distrib 8.0.19, for Win64 (x86_64)
--
-- Host: 127.0.0.1    Database: stockdata
-- ------------------------------------------------------
-- Server version	8.0.19

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `adj_factor`
--

DROP TABLE IF EXISTS `adj_factor`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `adj_factor` (
  `ts_code` varchar(9) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  `trade_date` date NOT NULL,
  `adj_factor` float DEFAULT NULL,
  PRIMARY KEY (`ts_code`,`trade_date`),
  KEY `ix_tradedate` (`trade_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `balancesheet`
--

DROP TABLE IF EXISTS `balancesheet`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `balancesheet` (
  `ts_code` char(9) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  `ann_date` date DEFAULT NULL,
  `f_ann_date` date DEFAULT NULL,
  `end_date` date NOT NULL,
  `report_type` varchar(2) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  `comp_type` char(1) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,
  `total_share` float DEFAULT NULL,
  `cap_rese` float DEFAULT NULL,
  `undistr_porfit` float DEFAULT NULL,
  `surplus_rese` float DEFAULT NULL,
  `special_rese` float DEFAULT NULL,
  `money_cap` float DEFAULT NULL,
  `trad_asset` float DEFAULT NULL,
  `notes_receiv` float DEFAULT NULL,
  `accounts_receiv` float DEFAULT NULL,
  `oth_receiv` float DEFAULT NULL,
  `prepayment` float DEFAULT NULL,
  `div_receiv` float DEFAULT NULL,
  `int_receiv` float DEFAULT NULL,
  `inventories` float DEFAULT NULL,
  `amor_exp` float DEFAULT NULL,
  `nca_within_1y` float DEFAULT NULL,
  `sett_rsrv` float DEFAULT NULL,
  `loanto_oth_bank_fi` float DEFAULT NULL,
  `premium_receiv` float DEFAULT NULL,
  `reinsur_receiv` float DEFAULT NULL,
  `reinsur_res_receiv` float DEFAULT NULL,
  `pur_resale_fa` float DEFAULT NULL,
  `oth_cur_assets` float DEFAULT NULL,
  `total_cur_assets` float DEFAULT NULL,
  `fa_avail_for_sale` float DEFAULT NULL,
  `htm_invest` float DEFAULT NULL,
  `lt_eqt_invest` float DEFAULT NULL,
  `invest_real_estate` float DEFAULT NULL,
  `time_deposits` float DEFAULT NULL,
  `oth_assets` float DEFAULT NULL,
  `lt_rec` float DEFAULT NULL,
  `fix_assets` float DEFAULT NULL,
  `cip` float DEFAULT NULL,
  `const_materials` float DEFAULT NULL,
  `fixed_assets_disp` float DEFAULT NULL,
  `produc_bio_assets` float DEFAULT NULL,
  `oil_and_gas_assets` float DEFAULT NULL,
  `intan_assets` float DEFAULT NULL,
  `r_and_d` float DEFAULT NULL,
  `goodwill` float DEFAULT NULL,
  `lt_amor_exp` float DEFAULT NULL,
  `defer_tax_assets` float DEFAULT NULL,
  `decr_in_disbur` float DEFAULT NULL,
  `oth_nca` float DEFAULT NULL,
  `total_nca` float DEFAULT NULL,
  `cash_reser_cb` float DEFAULT NULL,
  `depos_in_oth_bfi` float DEFAULT NULL,
  `prec_metals` float DEFAULT NULL,
  `deriv_assets` float DEFAULT NULL,
  `rr_reins_une_prem` float DEFAULT NULL,
  `rr_reins_outstd_cla` float DEFAULT NULL,
  `rr_reins_lins_liab` float DEFAULT NULL,
  `rr_reins_lthins_liab` float DEFAULT NULL,
  `refund_depos` float DEFAULT NULL,
  `ph_pledge_loans` float DEFAULT NULL,
  `refund_cap_depos` float DEFAULT NULL,
  `indep_acct_assets` float DEFAULT NULL,
  `client_depos` float DEFAULT NULL,
  `client_prov` float DEFAULT NULL,
  `transac_seat_fee` float DEFAULT NULL,
  `invest_as_receiv` float DEFAULT NULL,
  `total_assets` float DEFAULT NULL,
  `lt_borr` float DEFAULT NULL,
  `st_borr` float DEFAULT NULL,
  `cb_borr` float DEFAULT NULL,
  `depos_ib_deposits` float DEFAULT NULL,
  `loan_oth_bank` float DEFAULT NULL,
  `trading_fl` float DEFAULT NULL,
  `notes_payable` float DEFAULT NULL,
  `acct_payable` float DEFAULT NULL,
  `adv_receipts` float DEFAULT NULL,
  `sold_for_repur_fa` float DEFAULT NULL,
  `comm_payable` float DEFAULT NULL,
  `payroll_payable` float DEFAULT NULL,
  `taxes_payable` float DEFAULT NULL,
  `int_payable` float DEFAULT NULL,
  `div_payable` float DEFAULT NULL,
  `oth_payable` float DEFAULT NULL,
  `acc_exp` float DEFAULT NULL,
  `deferred_inc` float DEFAULT NULL,
  `st_bonds_payable` float DEFAULT NULL,
  `payable_to_reinsurer` float DEFAULT NULL,
  `rsrv_insur_cont` float DEFAULT NULL,
  `acting_trading_sec` float DEFAULT NULL,
  `acting_uw_sec` float DEFAULT NULL,
  `non_cur_liab_due_1y` float DEFAULT NULL,
  `oth_cur_liab` float DEFAULT NULL,
  `total_cur_liab` float DEFAULT NULL,
  `bond_payable` float DEFAULT NULL,
  `lt_payable` float DEFAULT NULL,
  `specific_payables` float DEFAULT NULL,
  `estimated_liab` float DEFAULT NULL,
  `defer_tax_liab` float DEFAULT NULL,
  `defer_inc_non_cur_liab` float DEFAULT NULL,
  `oth_ncl` float DEFAULT NULL,
  `total_ncl` float DEFAULT NULL,
  `depos_oth_bfi` float DEFAULT NULL,
  `deriv_liab` float DEFAULT NULL,
  `depos` float DEFAULT NULL,
  `agency_bus_liab` float DEFAULT NULL,
  `oth_liab` float DEFAULT NULL,
  `prem_receiv_adva` float DEFAULT NULL,
  `depos_received` float DEFAULT NULL,
  `ph_invest` float DEFAULT NULL,
  `reser_une_prem` float DEFAULT NULL,
  `reser_outstd_claims` float DEFAULT NULL,
  `reser_lins_liab` float DEFAULT NULL,
  `reser_lthins_liab` float DEFAULT NULL,
  `indept_acc_liab` float DEFAULT NULL,
  `pledge_borr` float DEFAULT NULL,
  `indem_payable` float DEFAULT NULL,
  `policy_div_payable` float DEFAULT NULL,
  `total_liab` float DEFAULT NULL,
  `treasury_share` float DEFAULT NULL,
  `ordin_risk_reser` float DEFAULT NULL,
  `forex_differ` float DEFAULT NULL,
  `invest_loss_unconf` float DEFAULT NULL,
  `minority_int` float DEFAULT NULL,
  `total_hldr_eqy_exc_min_int` float DEFAULT NULL,
  `total_hldr_eqy_inc_min_int` float DEFAULT NULL,
  `total_liab_hldr_eqy` float DEFAULT NULL,
  `lt_payroll_payable` float DEFAULT NULL,
  `oth_comp_income` float DEFAULT NULL,
  `oth_eqt_tools` float DEFAULT NULL,
  `oth_eqt_tools_p_shr` float DEFAULT NULL,
  `lending_funds` float DEFAULT NULL,
  `acc_receivable` float DEFAULT NULL,
  `st_fin_payable` float DEFAULT NULL,
  `payables` float DEFAULT NULL,
  `hfs_assets` float DEFAULT NULL,
  `hfs_sales` float DEFAULT NULL,
  `update_flag` char(1) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,
  PRIMARY KEY (`ts_code`,`end_date`,`report_type`),
  KEY `ix_anndate` (`ann_date`) /*!80000 INVISIBLE */,
  KEY `ix_enddate` (`end_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `cashflow`
--

DROP TABLE IF EXISTS `cashflow`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `cashflow` (
  `ts_code` char(9) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  `ann_date` date DEFAULT NULL,
  `f_ann_date` date DEFAULT NULL,
  `end_date` date NOT NULL,
  `comp_type` char(1) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,
  `report_type` varchar(2) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,
  `net_profit` float DEFAULT NULL,
  `finan_exp` float DEFAULT NULL,
  `c_fr_sale_sg` float DEFAULT NULL,
  `recp_tax_rends` float DEFAULT NULL,
  `n_depos_incr_fi` float DEFAULT NULL,
  `n_incr_loans_cb` float DEFAULT NULL,
  `n_inc_borr_oth_fi` float DEFAULT NULL,
  `prem_fr_orig_contr` float DEFAULT NULL,
  `n_incr_insured_dep` float DEFAULT NULL,
  `n_reinsur_prem` float DEFAULT NULL,
  `n_incr_disp_tfa` float DEFAULT NULL,
  `ifc_cash_incr` float DEFAULT NULL,
  `n_incr_disp_faas` float DEFAULT NULL,
  `n_incr_loans_oth_bank` float DEFAULT NULL,
  `n_cap_incr_repur` float DEFAULT NULL,
  `c_fr_oth_operate_a` float DEFAULT NULL,
  `c_inf_fr_operate_a` float DEFAULT NULL,
  `c_paid_goods_s` float DEFAULT NULL,
  `c_paid_to_for_empl` float DEFAULT NULL,
  `c_paid_for_taxes` float DEFAULT NULL,
  `n_incr_clt_loan_adv` float DEFAULT NULL,
  `n_incr_dep_cbob` float DEFAULT NULL,
  `c_pay_claims_orig_inco` float DEFAULT NULL,
  `pay_handling_chrg` float DEFAULT NULL,
  `pay_comm_insur_plcy` float DEFAULT NULL,
  `oth_cash_pay_oper_act` float DEFAULT NULL,
  `st_cash_out_act` float DEFAULT NULL,
  `n_cashflow_act` float DEFAULT NULL,
  `oth_recp_ral_inv_act` float DEFAULT NULL,
  `c_disp_withdrwl_invest` float DEFAULT NULL,
  `c_recp_return_invest` float DEFAULT NULL,
  `n_recp_disp_fiolta` float DEFAULT NULL,
  `n_recp_disp_sobu` float DEFAULT NULL,
  `stot_inflows_inv_act` float DEFAULT NULL,
  `c_pay_acq_const_fiolta` float DEFAULT NULL,
  `c_paid_invest` float DEFAULT NULL,
  `n_disp_subs_oth_biz` float DEFAULT NULL,
  `oth_pay_ral_inv_act` float DEFAULT NULL,
  `n_incr_pledge_loan` float DEFAULT NULL,
  `stot_out_inv_act` float DEFAULT NULL,
  `n_cashflow_inv_act` float DEFAULT NULL,
  `c_recp_borrow` float DEFAULT NULL,
  `proc_issue_bonds` float DEFAULT NULL,
  `oth_cash_recp_ral_fnc_act` float DEFAULT NULL,
  `stot_cash_in_fnc_act` float DEFAULT NULL,
  `free_cashflow` float DEFAULT NULL,
  `c_prepay_amt_borr` float DEFAULT NULL,
  `c_pay_dist_dpcp_int_exp` float DEFAULT NULL,
  `incl_dvd_profit_paid_sc_ms` float DEFAULT NULL,
  `oth_cashpay_ral_fnc_act` float DEFAULT NULL,
  `stot_cashout_fnc_act` float DEFAULT NULL,
  `n_cash_flows_fnc_act` float DEFAULT NULL,
  `eff_fx_flu_cash` float DEFAULT NULL,
  `n_incr_cash_cash_equ` float DEFAULT NULL,
  `c_cash_equ_beg_period` float DEFAULT NULL,
  `c_cash_equ_end_period` float DEFAULT NULL,
  `c_recp_cap_contrib` float DEFAULT NULL,
  `incl_cash_rec_saims` float DEFAULT NULL,
  `uncon_invest_loss` float DEFAULT NULL,
  `prov_depr_assets` float DEFAULT NULL,
  `depr_fa_coga_dpba` float DEFAULT NULL,
  `amort_intang_assets` float DEFAULT NULL,
  `lt_amort_deferred_exp` float DEFAULT NULL,
  `decr_deferred_exp` float DEFAULT NULL,
  `incr_acc_exp` float DEFAULT NULL,
  `loss_disp_fiolta` float DEFAULT NULL,
  `loss_scr_fa` float DEFAULT NULL,
  `loss_fv_chg` float DEFAULT NULL,
  `invest_loss` float DEFAULT NULL,
  `decr_def_inc_tax_assets` float DEFAULT NULL,
  `incr_def_inc_tax_liab` float DEFAULT NULL,
  `decr_inventories` float DEFAULT NULL,
  `decr_oper_payable` float DEFAULT NULL,
  `incr_oper_payable` float DEFAULT NULL,
  `others` float DEFAULT NULL,
  `im_net_cashflow_oper_act` float DEFAULT NULL,
  `conv_debt_into_cap` float DEFAULT NULL,
  `conv_copbonds_due_within_1y` float DEFAULT NULL,
  `fa_fnc_leases` float DEFAULT NULL,
  `end_bal_cash` float DEFAULT NULL,
  `beg_bal_cash` float DEFAULT NULL,
  `end_bal_cash_equ` float DEFAULT NULL,
  `beg_bal_cash_equ` float DEFAULT NULL,
  `im_n_incr_cash_equ` float DEFAULT NULL,
  `update_flag` char(1) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,
  PRIMARY KEY (`ts_code`,`end_date`),
  KEY `ix_enddate` (`end_date`) /*!80000 INVISIBLE */,
  KEY `ix_anndate` (`ann_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `chigu`
--

DROP TABLE IF EXISTS `chigu`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `chigu` (
  `ts_code` varchar(9) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL DEFAULT '',
  PRIMARY KEY (`ts_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `chiguguzhi`
--

DROP TABLE IF EXISTS `chiguguzhi`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `chiguguzhi` (
  `ts_code` varchar(9) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL DEFAULT '',
  `name` varchar(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `pe` double DEFAULT NULL,
  `peg` double DEFAULT NULL,
  `next1YearPE` double DEFAULT NULL,
  `next2YearPE` double DEFAULT NULL,
  `next3YearPE` double DEFAULT NULL,
  `incrate0` double DEFAULT NULL,
  `incrate1` double DEFAULT NULL,
  `incrate2` double DEFAULT NULL,
  `incrate3` double DEFAULT NULL,
  `incrate4` double DEFAULT NULL,
  `incrate5` double DEFAULT NULL,
  `avgrate` double DEFAULT NULL,
  `madrate` double DEFAULT NULL,
  `stdrate` double DEFAULT NULL,
  `pe200` double DEFAULT NULL,
  `pe1000` double DEFAULT NULL,
  PRIMARY KEY (`ts_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `classify`
--

DROP TABLE IF EXISTS `classify`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `classify` (
  `code` varchar(8) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL DEFAULT '',
  `name` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `level` int DEFAULT NULL,
  `level1id` varchar(2) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `level2id` varchar(4) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `level3id` varchar(6) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  PRIMARY KEY (`code`),
  KEY `hyid` (`code`),
  KEY `hylevel1id` (`level1id`),
  KEY `hylevel2id` (`level2id`),
  KEY `hylevel3id` (`level3id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `classify_member`
--

DROP TABLE IF EXISTS `classify_member`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `classify_member` (
  `ts_code` varchar(9) COLLATE utf8mb4_bin NOT NULL DEFAULT '',
  `date` date NOT NULL,
  `classify_code` varchar(8) COLLATE utf8mb4_bin DEFAULT NULL,
  PRIMARY KEY (`ts_code`,`date`),
  KEY `ix_code_date` (`classify_code`,`date`),
  KEY `ix_date` (`date`) /*!80000 INVISIBLE */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `classify_pe`
--

DROP TABLE IF EXISTS `classify_pe`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `classify_pe` (
  `code` varchar(8) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `date` date NOT NULL,
  `pe` float DEFAULT NULL,
  PRIMARY KEY (`code`,`date`),
  KEY `index_date` (`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `classify_profits`
--

DROP TABLE IF EXISTS `classify_profits`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `classify_profits` (
  `code` varchar(8) COLLATE utf8mb4_bin NOT NULL DEFAULT '',
  `end_date` date NOT NULL,
  `profits` double DEFAULT NULL,
  `profitsLast` double DEFAULT NULL,
  `profitsInc` double DEFAULT NULL,
  `profitsIncRate` double DEFAULT NULL,
  PRIMARY KEY (`code`,`end_date`),
  KEY `ix_end_date` (`end_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `daily`
--

DROP TABLE IF EXISTS `daily`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `daily` (
  `ts_code` varchar(9) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  `trade_date` date NOT NULL,
  `open` float NOT NULL,
  `high` float NOT NULL,
  `low` float NOT NULL,
  `close` float NOT NULL,
  `pre_close` float NOT NULL,
  `change` float NOT NULL,
  `pct_chg` float NOT NULL,
  `vol` float NOT NULL,
  `amount` float NOT NULL,
  PRIMARY KEY (`ts_code`,`trade_date`),
  KEY `ix_tradedate` (`trade_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `daily_basic`
--

DROP TABLE IF EXISTS `daily_basic`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `daily_basic` (
  `ts_code` varchar(9) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  `trade_date` date NOT NULL,
  `close` float DEFAULT NULL,
  `turnover_rate` float DEFAULT NULL,
  `turnover_rate_f` float DEFAULT NULL,
  `volume_ratio` float DEFAULT NULL,
  `pe` float DEFAULT NULL,
  `pe_ttm` float DEFAULT NULL,
  `pb` float DEFAULT NULL,
  `ps` float DEFAULT NULL,
  `ps_ttm` float DEFAULT NULL,
  `dv_ratio` float DEFAULT NULL,
  `dv_ttm` float DEFAULT NULL,
  `total_share` float DEFAULT NULL,
  `float_share` float DEFAULT NULL,
  `free_share` float DEFAULT NULL,
  `total_mv` float DEFAULT NULL,
  `circ_mv` float DEFAULT NULL,
  PRIMARY KEY (`ts_code`,`trade_date`),
  KEY `index_date` (`trade_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `disclosure_date`
--

DROP TABLE IF EXISTS `disclosure_date`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `disclosure_date` (
  `ts_code` char(9) COLLATE utf8mb4_bin NOT NULL,
  `ann_date` date DEFAULT NULL,
  `end_date` date NOT NULL,
  `pre_date` date DEFAULT NULL,
  `actual_date` date DEFAULT NULL,
  `modify_date` date DEFAULT NULL,
  PRIMARY KEY (`ts_code`,`end_date`),
  KEY `ix_enddate` (`end_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dividend`
--

DROP TABLE IF EXISTS `dividend`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `dividend` (
  `ts_code` char(9) COLLATE utf8mb4_bin NOT NULL,
  `end_date` date NOT NULL,
  `ann_date` date DEFAULT NULL,
  `div_proc` varchar(20) COLLATE utf8mb4_bin NOT NULL,
  `stk_div` float DEFAULT NULL,
  `stk_bo_rate` float DEFAULT NULL,
  `stk_co_rate` float DEFAULT NULL,
  `cash_div` float DEFAULT NULL,
  `cash_div_tax` float DEFAULT NULL,
  `record_date` date DEFAULT NULL,
  `ex_date` date DEFAULT NULL,
  `pay_date` date DEFAULT NULL,
  `div_listdate` date DEFAULT NULL,
  `imp_ann_date` date DEFAULT NULL,
  `base_date` date DEFAULT NULL,
  `base_share` float DEFAULT NULL,
  PRIMARY KEY (`ts_code`,`end_date`,`div_proc`),
  KEY `ix_enddate` (`end_date`),
  KEY `ix_anndate` (`ann_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `express`
--

DROP TABLE IF EXISTS `express`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `express` (
  `ts_code` char(9) COLLATE utf8mb4_bin NOT NULL,
  `ann_date` date DEFAULT NULL,
  `end_date` date NOT NULL,
  `revenue` float DEFAULT NULL,
  `operate_profit` float DEFAULT NULL,
  `total_profit` float DEFAULT NULL,
  `n_income` float DEFAULT NULL,
  `total_assets` float DEFAULT NULL,
  `total_hldr_eqy_exc_min_int` float DEFAULT NULL,
  `diluted_eps` float DEFAULT NULL,
  `diluted_roe` float DEFAULT NULL,
  `yoy_net_profit` float DEFAULT NULL,
  `bps` float DEFAULT NULL,
  `yoy_sales` float DEFAULT NULL,
  `yoy_op` float DEFAULT NULL,
  `yoy_tp` float DEFAULT NULL,
  `yoy_dedu_np` float DEFAULT NULL,
  `yoy_eps` float DEFAULT NULL,
  `yoy_roe` float DEFAULT NULL,
  `growth_assets` float DEFAULT NULL,
  `yoy_equity` float DEFAULT NULL,
  `growth_bps` float DEFAULT NULL,
  `or_last_year` float DEFAULT NULL,
  `op_last_year` float DEFAULT NULL,
  `tp_last_year` float DEFAULT NULL,
  `np_last_year` float DEFAULT NULL,
  `eps_last_year` float DEFAULT NULL,
  `open_net_assets` float DEFAULT NULL,
  `open_bps` float DEFAULT NULL,
  `perf_summary` varchar(1000) COLLATE utf8mb4_bin DEFAULT NULL,
  `is_audit` int DEFAULT NULL,
  `remark` varchar(1000) COLLATE utf8mb4_bin DEFAULT NULL,
  PRIMARY KEY (`ts_code`,`end_date`),
  KEY `ix_enddate` (`end_date`) /*!80000 INVISIBLE */,
  KEY `ix_anndate` (`ann_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `fina_indicator`
--

DROP TABLE IF EXISTS `fina_indicator`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `fina_indicator` (
  `ts_code` char(9) COLLATE utf8mb4_bin NOT NULL,
  `ann_date` date DEFAULT NULL,
  `end_date` date NOT NULL,
  `eps` float DEFAULT NULL,
  `dt_eps` float DEFAULT NULL,
  `total_revenue_ps` float DEFAULT NULL,
  `revenue_ps` float DEFAULT NULL,
  `capital_rese_ps` float DEFAULT NULL,
  `surplus_rese_ps` float DEFAULT NULL,
  `undist_profit_ps` float DEFAULT NULL,
  `extra_item` float DEFAULT NULL,
  `profit_dedt` float DEFAULT NULL,
  `gross_margin` float DEFAULT NULL,
  `current_ratio` float DEFAULT NULL,
  `quick_ratio` float DEFAULT NULL,
  `cash_ratio` float DEFAULT NULL,
  `invturn_days` float DEFAULT NULL,
  `arturn_days` float DEFAULT NULL,
  `inv_turn` float DEFAULT NULL,
  `ar_turn` float DEFAULT NULL,
  `ca_turn` float DEFAULT NULL,
  `fa_turn` float DEFAULT NULL,
  `assets_turn` float DEFAULT NULL,
  `op_income` float DEFAULT NULL,
  `valuechange_income` float DEFAULT NULL,
  `interst_income` float DEFAULT NULL,
  `daa` float DEFAULT NULL,
  `ebit` float DEFAULT NULL,
  `ebitda` float DEFAULT NULL,
  `fcff` float DEFAULT NULL,
  `fcfe` float DEFAULT NULL,
  `current_exint` float DEFAULT NULL,
  `noncurrent_exint` float DEFAULT NULL,
  `interestdebt` float DEFAULT NULL,
  `netdebt` float DEFAULT NULL,
  `tangible_asset` float DEFAULT NULL,
  `working_capital` float DEFAULT NULL,
  `networking_capital` float DEFAULT NULL,
  `invest_capital` float DEFAULT NULL,
  `retained_earnings` float DEFAULT NULL,
  `diluted2_eps` float DEFAULT NULL,
  `bps` float DEFAULT NULL,
  `ocfps` float DEFAULT NULL,
  `retainedps` float DEFAULT NULL,
  `cfps` float DEFAULT NULL,
  `ebit_ps` float DEFAULT NULL,
  `fcff_ps` float DEFAULT NULL,
  `fcfe_ps` float DEFAULT NULL,
  `netprofit_margin` float DEFAULT NULL,
  `grossprofit_margin` float DEFAULT NULL,
  `cogs_of_sales` float DEFAULT NULL,
  `expense_of_sales` float DEFAULT NULL,
  `profit_to_gr` float DEFAULT NULL,
  `saleexp_to_gr` float DEFAULT NULL,
  `adminexp_of_gr` float DEFAULT NULL,
  `finaexp_of_gr` float DEFAULT NULL,
  `impai_ttm` float DEFAULT NULL,
  `gc_of_gr` float DEFAULT NULL,
  `op_of_gr` float DEFAULT NULL,
  `ebit_of_gr` float DEFAULT NULL,
  `roe` float DEFAULT NULL,
  `roe_waa` float DEFAULT NULL,
  `roe_dt` float DEFAULT NULL,
  `roa` float DEFAULT NULL,
  `npta` float DEFAULT NULL,
  `roic` float DEFAULT NULL,
  `roe_yearly` float DEFAULT NULL,
  `roa2_yearly` float DEFAULT NULL,
  `roe_avg` float DEFAULT NULL,
  `opincome_of_ebt` float DEFAULT NULL,
  `investincome_of_ebt` float DEFAULT NULL,
  `n_op_profit_of_ebt` float DEFAULT NULL,
  `tax_to_ebt` float DEFAULT NULL,
  `dtprofit_to_profit` float DEFAULT NULL,
  `salescash_to_or` float DEFAULT NULL,
  `ocf_to_or` float DEFAULT NULL,
  `ocf_to_opincome` float DEFAULT NULL,
  `capitalized_to_da` float DEFAULT NULL,
  `debt_to_assets` float DEFAULT NULL,
  `assets_to_eqt` float DEFAULT NULL,
  `dp_assets_to_eqt` float DEFAULT NULL,
  `ca_to_assets` float DEFAULT NULL,
  `nca_to_assets` float DEFAULT NULL,
  `tbassets_to_totalassets` float DEFAULT NULL,
  `int_to_talcap` float DEFAULT NULL,
  `eqt_to_talcapital` float DEFAULT NULL,
  `currentdebt_to_debt` float DEFAULT NULL,
  `longdeb_to_debt` float DEFAULT NULL,
  `ocf_to_shortdebt` float DEFAULT NULL,
  `debt_to_eqt` float DEFAULT NULL,
  `eqt_to_debt` float DEFAULT NULL,
  `eqt_to_interestdebt` float DEFAULT NULL,
  `tangibleasset_to_debt` float DEFAULT NULL,
  `tangasset_to_intdebt` float DEFAULT NULL,
  `tangibleasset_to_netdebt` float DEFAULT NULL,
  `ocf_to_debt` float DEFAULT NULL,
  `ocf_to_interestdebt` float DEFAULT NULL,
  `ocf_to_netdebt` float DEFAULT NULL,
  `ebit_to_interest` float DEFAULT NULL,
  `longdebt_to_workingcapital` float DEFAULT NULL,
  `ebitda_to_debt` float DEFAULT NULL,
  `turn_days` float DEFAULT NULL,
  `roa_yearly` float DEFAULT NULL,
  `roa_dp` float DEFAULT NULL,
  `fixed_assets` float DEFAULT NULL,
  `profit_prefin_exp` float DEFAULT NULL,
  `non_op_profit` float DEFAULT NULL,
  `op_to_ebt` float DEFAULT NULL,
  `nop_to_ebt` float DEFAULT NULL,
  `ocf_to_profit` float DEFAULT NULL,
  `cash_to_liqdebt` float DEFAULT NULL,
  `cash_to_liqdebt_withinterest` float DEFAULT NULL,
  `op_to_liqdebt` float DEFAULT NULL,
  `op_to_debt` float DEFAULT NULL,
  `roic_yearly` float DEFAULT NULL,
  `total_fa_trun` float DEFAULT NULL,
  `profit_to_op` float DEFAULT NULL,
  `q_opincome` float DEFAULT NULL,
  `q_investincome` float DEFAULT NULL,
  `q_dtprofit` float DEFAULT NULL,
  `q_eps` float DEFAULT NULL,
  `q_netprofit_margin` float DEFAULT NULL,
  `q_gsprofit_margin` float DEFAULT NULL,
  `q_exp_to_sales` float DEFAULT NULL,
  `q_profit_to_gr` float DEFAULT NULL,
  `q_saleexp_to_gr` float DEFAULT NULL,
  `q_adminexp_to_gr` float DEFAULT NULL,
  `q_finaexp_to_gr` float DEFAULT NULL,
  `q_impair_to_gr_ttm` float DEFAULT NULL,
  `q_gc_to_gr` float DEFAULT NULL,
  `q_op_to_gr` float DEFAULT NULL,
  `q_roe` float DEFAULT NULL,
  `q_dt_roe` float DEFAULT NULL,
  `q_npta` float DEFAULT NULL,
  `q_opincome_to_ebt` float DEFAULT NULL,
  `q_investincome_to_ebt` float DEFAULT NULL,
  `q_dtprofit_to_profit` float DEFAULT NULL,
  `q_salescash_to_or` float DEFAULT NULL,
  `q_ocf_to_sales` float DEFAULT NULL,
  `q_ocf_to_or` float DEFAULT NULL,
  `basic_eps_yoy` float DEFAULT NULL,
  `dt_eps_yoy` float DEFAULT NULL,
  `cfps_yoy` float DEFAULT NULL,
  `op_yoy` float DEFAULT NULL,
  `ebt_yoy` float DEFAULT NULL,
  `netprofit_yoy` float DEFAULT NULL,
  `dt_netprofit_yoy` float DEFAULT NULL,
  `ocf_yoy` float DEFAULT NULL,
  `roe_yoy` float DEFAULT NULL,
  `bps_yoy` float DEFAULT NULL,
  `assets_yoy` float DEFAULT NULL,
  `eqt_yoy` float DEFAULT NULL,
  `tr_yoy` float DEFAULT NULL,
  `or_yoy` float DEFAULT NULL,
  `q_gr_yoy` float DEFAULT NULL,
  `q_gr_qoq` float DEFAULT NULL,
  `q_sales_yoy` float DEFAULT NULL,
  `q_sales_qoq` float DEFAULT NULL,
  `q_op_yoy` float DEFAULT NULL,
  `q_op_qoq` float DEFAULT NULL,
  `q_profit_yoy` float DEFAULT NULL,
  `q_profit_qoq` float DEFAULT NULL,
  `q_netprofit_yoy` float DEFAULT NULL,
  `q_netprofit_qoq` float DEFAULT NULL,
  `equity_yoy` float DEFAULT NULL,
  `rd_exp` float DEFAULT NULL,
  `update_flag` char(1) COLLATE utf8mb4_bin DEFAULT NULL,
  PRIMARY KEY (`ts_code`,`end_date`),
  KEY `ix_enddate` (`end_date`),
  KEY `ix_anndate` (`ann_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `forecast`
--

DROP TABLE IF EXISTS `forecast`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `forecast` (
  `ts_code` char(9) COLLATE utf8mb4_bin NOT NULL,
  `ann_date` date DEFAULT NULL,
  `end_date` date NOT NULL,
  `type` varchar(20) COLLATE utf8mb4_bin DEFAULT NULL,
  `p_change_min` float DEFAULT NULL,
  `p_change_max` float DEFAULT NULL,
  `net_profit_min` float DEFAULT NULL,
  `net_profit_max` float DEFAULT NULL,
  `last_parent_net` float DEFAULT NULL,
  `first_ann_date` date DEFAULT NULL,
  `summary` varchar(1000) COLLATE utf8mb4_bin DEFAULT NULL,
  `change_reason` varchar(3000) COLLATE utf8mb4_bin DEFAULT NULL,
  PRIMARY KEY (`ts_code`,`end_date`),
  KEY `ix_enddate` (`end_date`),
  KEY `ix_anndate` (`ann_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `fund`
--

DROP TABLE IF EXISTS `fund`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `fund` (
  `ts_code` varchar(9) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,
  `trade_date` date DEFAULT NULL,
  `pre_close` double DEFAULT NULL,
  `open` double DEFAULT NULL,
  `high` double DEFAULT NULL,
  `low` double DEFAULT NULL,
  `close` double DEFAULT NULL,
  `change` double DEFAULT NULL,
  `pct_chg` double DEFAULT NULL,
  `vol` double DEFAULT NULL,
  `amount` double DEFAULT NULL,
  KEY `ix_fund_index_date` (`ts_code`,`trade_date`) /*!80000 INVISIBLE */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `guzhi`
--

DROP TABLE IF EXISTS `guzhi`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `guzhi` (
  `ts_code` varchar(9) COLLATE utf8mb4_bin NOT NULL DEFAULT '',
  `peg` double DEFAULT NULL,
  `next1YearPE` double DEFAULT NULL,
  `next2YearPE` double DEFAULT NULL,
  `next3YearPE` double DEFAULT NULL,
  PRIMARY KEY (`ts_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `guzhihistorystatus`
--

DROP TABLE IF EXISTS `guzhihistorystatus`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `guzhihistorystatus` (
  `ts_code` varchar(9) COLLATE utf8mb4_bin NOT NULL DEFAULT '',
  `date` int NOT NULL,
  `integrity` tinyint(1) DEFAULT NULL,
  `seculargrowth` tinyint(1) DEFAULT NULL,
  `growthmadrate` float DEFAULT NULL,
  `averageincrement` float DEFAULT NULL,
  PRIMARY KEY (`ts_code`,`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `guzhiresult`
--

DROP TABLE IF EXISTS `guzhiresult`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `guzhiresult` (
  `ts_code` varchar(9) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL DEFAULT '',
  `name` varchar(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `pe` double DEFAULT NULL,
  `peg` double DEFAULT NULL,
  `next1YearPE` double DEFAULT NULL,
  `next2YearPE` double DEFAULT NULL,
  `next3YearPE` double DEFAULT NULL,
  `incrate0` double DEFAULT NULL,
  `incrate1` double DEFAULT NULL,
  `incrate2` double DEFAULT NULL,
  `incrate3` double DEFAULT NULL,
  `incrate4` double DEFAULT NULL,
  `incrate5` double DEFAULT NULL,
  `avgrate` double DEFAULT NULL,
  `madrate` double DEFAULT NULL,
  `stdrate` double DEFAULT NULL,
  `pe200` double DEFAULT NULL,
  `pe1000` double DEFAULT NULL,
  PRIMARY KEY (`ts_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `income`
--

DROP TABLE IF EXISTS `income`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `income` (
  `ts_code` char(9) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  `ann_date` date DEFAULT NULL,
  `f_ann_date` date DEFAULT NULL,
  `end_date` date NOT NULL,
  `report_type` varchar(2) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,
  `comp_type` char(1) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,
  `basic_eps` float DEFAULT NULL,
  `diluted_eps` float DEFAULT NULL,
  `total_revenue` float DEFAULT NULL,
  `revenue` float DEFAULT NULL,
  `int_income` float DEFAULT NULL,
  `prem_earned` float DEFAULT NULL,
  `comm_income` float DEFAULT NULL,
  `n_commis_income` float DEFAULT NULL,
  `n_oth_income` float DEFAULT NULL,
  `n_oth_b_income` float DEFAULT NULL,
  `prem_income` float DEFAULT NULL,
  `out_prem` float DEFAULT NULL,
  `une_prem_reser` float DEFAULT NULL,
  `reins_income` float DEFAULT NULL,
  `n_sec_tb_income` float DEFAULT NULL,
  `n_sec_uw_income` float DEFAULT NULL,
  `n_asset_mg_income` float DEFAULT NULL,
  `oth_b_income` float DEFAULT NULL,
  `fv_value_chg_gain` float DEFAULT NULL,
  `invest_income` float DEFAULT NULL,
  `ass_invest_income` float DEFAULT NULL,
  `forex_gain` float DEFAULT NULL,
  `total_cogs` float DEFAULT NULL,
  `oper_cost` float DEFAULT NULL,
  `int_exp` float DEFAULT NULL,
  `comm_exp` float DEFAULT NULL,
  `biz_tax_surchg` float DEFAULT NULL,
  `sell_exp` float DEFAULT NULL,
  `admin_exp` float DEFAULT NULL,
  `fin_exp` float DEFAULT NULL,
  `assets_impair_loss` float DEFAULT NULL,
  `prem_refund` float DEFAULT NULL,
  `compens_payout` float DEFAULT NULL,
  `reser_insur_liab` float DEFAULT NULL,
  `div_payt` float DEFAULT NULL,
  `reins_exp` float DEFAULT NULL,
  `oper_exp` float DEFAULT NULL,
  `compens_payout_refu` float DEFAULT NULL,
  `insur_reser_refu` float DEFAULT NULL,
  `reins_cost_refund` float DEFAULT NULL,
  `other_bus_cost` float DEFAULT NULL,
  `operate_profit` float DEFAULT NULL,
  `non_oper_income` float DEFAULT NULL,
  `non_oper_exp` float DEFAULT NULL,
  `nca_disploss` float DEFAULT NULL,
  `total_profit` float DEFAULT NULL,
  `income_tax` float DEFAULT NULL,
  `n_income` float DEFAULT NULL,
  `n_income_attr_p` float DEFAULT NULL,
  `minority_gain` float DEFAULT NULL,
  `oth_compr_income` float DEFAULT NULL,
  `t_compr_income` float DEFAULT NULL,
  `compr_inc_attr_p` float DEFAULT NULL,
  `compr_inc_attr_m_s` float DEFAULT NULL,
  `ebit` float DEFAULT NULL,
  `ebitda` float DEFAULT NULL,
  `insurance_exp` float DEFAULT NULL,
  `undist_profit` float DEFAULT NULL,
  `distable_profit` float DEFAULT NULL,
  `update_flag` char(1) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,
  PRIMARY KEY (`ts_code`,`end_date`),
  KEY `ix_enddate` (`end_date`),
  KEY `ix_anndate` (`ann_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `incomehistorya`
--

DROP TABLE IF EXISTS `incomehistorya`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `incomehistorya` (
  `date` date NOT NULL,
  `amount` float(10,2) DEFAULT NULL,
  `stocks` varchar(255) COLLATE utf8mb4_bin DEFAULT NULL,
  `totalvalue` float(10,2) DEFAULT NULL,
  PRIMARY KEY (`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `incomehistoryb`
--

DROP TABLE IF EXISTS `incomehistoryb`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `incomehistoryb` (
  `date` date NOT NULL,
  `amount` float(10,2) DEFAULT NULL,
  `stocks` varchar(255) COLLATE utf8mb4_bin DEFAULT NULL,
  `totalvalue` float(10,2) DEFAULT NULL,
  PRIMARY KEY (`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `index_basic`
--

DROP TABLE IF EXISTS `index_basic`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `index_basic` (
  `ts_code` char(9) COLLATE utf8mb4_bin NOT NULL,
  `name` varchar(60) COLLATE utf8mb4_bin DEFAULT NULL,
  `market` varchar(6) COLLATE utf8mb4_bin DEFAULT NULL,
  `publisher` varchar(6) COLLATE utf8mb4_bin DEFAULT NULL,
  `category` varchar(20) COLLATE utf8mb4_bin DEFAULT NULL,
  `base_date` date DEFAULT NULL,
  `base_point` float DEFAULT NULL,
  `list_date` date DEFAULT NULL,
  PRIMARY KEY (`ts_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `index_daily`
--

DROP TABLE IF EXISTS `index_daily`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `index_daily` (
  `ts_code` char(9) COLLATE utf8mb4_bin NOT NULL,
  `trade_date` date NOT NULL,
  `close` float DEFAULT NULL,
  `open` float DEFAULT NULL,
  `high` float DEFAULT NULL,
  `low` float DEFAULT NULL,
  `pre_close` float DEFAULT NULL,
  `change` float DEFAULT NULL,
  `pct_chg` float DEFAULT NULL,
  `vol` float DEFAULT NULL,
  `amount` float DEFAULT NULL,
  PRIMARY KEY (`ts_code`,`trade_date`),
  KEY `ix_tradedate` (`trade_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `index_dailybasic`
--

DROP TABLE IF EXISTS `index_dailybasic`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `index_dailybasic` (
  `ts_code` char(9) COLLATE utf8mb4_bin NOT NULL,
  `trade_date` date NOT NULL,
  `total_mv` float DEFAULT NULL,
  `float_mv` float DEFAULT NULL,
  `total_share` float DEFAULT NULL,
  `float_share` float DEFAULT NULL,
  `free_share` float DEFAULT NULL,
  `turnover_rate` float DEFAULT NULL,
  `turnover_rate_f` float DEFAULT NULL,
  `pe` float DEFAULT NULL,
  `pe_ttm` float DEFAULT NULL,
  `pb` float DEFAULT NULL,
  PRIMARY KEY (`ts_code`,`trade_date`),
  KEY `ix_tradedate` (`trade_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `index_pe`
--

DROP TABLE IF EXISTS `index_pe`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `index_pe` (
  `ts_code` varchar(9) COLLATE utf8mb4_bin NOT NULL,
  `trade_date` date NOT NULL,
  `pe_ttm` decimal(10,2) NOT NULL,
  PRIMARY KEY (`ts_code`,`trade_date`),
  KEY `ix_name` (`ts_code`),
  KEY `ix_date` (`trade_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `index_weight`
--

DROP TABLE IF EXISTS `index_weight`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `index_weight` (
  `index_code` varchar(9) COLLATE utf8mb4_bin NOT NULL,
  `con_code` char(9) COLLATE utf8mb4_bin NOT NULL,
  `trade_date` date NOT NULL,
  `weight` float DEFAULT NULL,
  PRIMARY KEY (`index_code`,`con_code`,`trade_date`),
  KEY `ix_tradedate` (`trade_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `pledge_stat`
--

DROP TABLE IF EXISTS `pledge_stat`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `pledge_stat` (
  `ts_code` varchar(9) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  `end_date` date NOT NULL,
  `pledge_count` float DEFAULT NULL,
  `unrest_pledge` float DEFAULT NULL,
  `rest_pledge` float DEFAULT NULL,
  `total_share` float DEFAULT NULL,
  `pledge_ratio` float DEFAULT NULL,
  PRIMARY KEY (`ts_code`,`end_date`),
  KEY `index_date` (`end_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
/*!50100 PARTITION BY RANGE (year(`end_date`))
(PARTITION part0 VALUES LESS THAN (2009) ENGINE = InnoDB,
 PARTITION part1 VALUES LESS THAN (2010) ENGINE = InnoDB,
 PARTITION part2 VALUES LESS THAN (2011) ENGINE = InnoDB,
 PARTITION part3 VALUES LESS THAN (2012) ENGINE = InnoDB,
 PARTITION part4 VALUES LESS THAN (2013) ENGINE = InnoDB,
 PARTITION part5 VALUES LESS THAN (2014) ENGINE = InnoDB,
 PARTITION part6 VALUES LESS THAN (2015) ENGINE = InnoDB,
 PARTITION part7 VALUES LESS THAN (2016) ENGINE = InnoDB,
 PARTITION part8 VALUES LESS THAN (2017) ENGINE = InnoDB,
 PARTITION part9 VALUES LESS THAN (2018) ENGINE = InnoDB,
 PARTITION part10 VALUES LESS THAN (2019) ENGINE = InnoDB,
 PARTITION part11 VALUES LESS THAN (2020) ENGINE = InnoDB,
 PARTITION part12 VALUES LESS THAN (2021) ENGINE = InnoDB,
 PARTITION part13 VALUES LESS THAN (2022) ENGINE = InnoDB,
 PARTITION part14 VALUES LESS THAN (2023) ENGINE = InnoDB,
 PARTITION part15 VALUES LESS THAN (2024) ENGINE = InnoDB,
 PARTITION part16 VALUES LESS THAN (2025) ENGINE = InnoDB,
 PARTITION part17 VALUES LESS THAN (2026) ENGINE = InnoDB,
 PARTITION part18 VALUES LESS THAN (2027) ENGINE = InnoDB,
 PARTITION part19 VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `stock_basic`
--

DROP TABLE IF EXISTS `stock_basic`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `stock_basic` (
  `ts_code` varchar(9) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  `symbol` varchar(6) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,
  `name` varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,
  `area` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,
  `industry` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,
  `fullname` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,
  `enname` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,
  `market` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,
  `exchange` varchar(6) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,
  `curr_type` varchar(6) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,
  `list_status` varchar(6) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,
  `list_date` date DEFAULT NULL,
  `delist_date` date DEFAULT NULL,
  `is_hs` varchar(6) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,
  PRIMARY KEY (`ts_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `trade_cal`
--

DROP TABLE IF EXISTS `trade_cal`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `trade_cal` (
  `exchange` varchar(8) COLLATE utf8mb4_bin NOT NULL,
  `cal_date` date NOT NULL,
  `is_open` tinyint(1) NOT NULL,
  `pretrade_date` date DEFAULT NULL,
  PRIMARY KEY (`exchange`,`cal_date`,`is_open`),
  KEY `ix_opencaldate` (`is_open`,`cal_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `ttmprofits`
--

DROP TABLE IF EXISTS `ttmprofits`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `ttmprofits` (
  `ts_code` varchar(9) COLLATE utf8mb4_bin NOT NULL,
  `end_date` date NOT NULL,
  `ttmprofits` double NOT NULL,
  `ann_date` date NOT NULL,
  `incrate` float DEFAULT NULL,
  PRIMARY KEY (`ts_code`,`end_date`),
  KEY `ix_ann_date` (`ann_date`),
  KEY `ix_end_date` (`end_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `update_date`
--

DROP TABLE IF EXISTS `update_date`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `update_date` (
  `id` int NOT NULL AUTO_INCREMENT,
  `dataname` varchar(45) NOT NULL,
  `date` date NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `dataname_UNIQUE` (`dataname`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `valuation`
--

DROP TABLE IF EXISTS `valuation`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `valuation` (
  `ts_code` varchar(9) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  `date` date NOT NULL,
  `name` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `pf` int DEFAULT NULL,
  `pe` float DEFAULT NULL,
  `lowpe` int DEFAULT NULL,
  `classify_code` varchar(8) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,
  `classify_pe` float DEFAULT NULL,
  `lowhype` int DEFAULT NULL,
  `incrate0` float DEFAULT NULL,
  `incrate1` float DEFAULT NULL,
  `incrate2` float DEFAULT NULL,
  `incrate3` float DEFAULT NULL,
  `incrate4` float DEFAULT NULL,
  `incrate5` float DEFAULT NULL,
  `avg` float DEFAULT NULL,
  `std` float DEFAULT NULL,
  `peg` float DEFAULT NULL,
  `lowpeg` int DEFAULT NULL,
  `wdzz` int DEFAULT NULL,
  `wdzz1` int DEFAULT NULL,
  `pez200` float DEFAULT NULL,
  `lowpez200` int DEFAULT NULL,
  `pez1000` float DEFAULT NULL,
  `lowpez1000` int DEFAULT NULL,
  `pe200` int DEFAULT NULL,
  `pe1000` int DEFAULT NULL,
  PRIMARY KEY (`ts_code`,`date`),
  KEY `index_date` (`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2020-11-16 11:06:09
