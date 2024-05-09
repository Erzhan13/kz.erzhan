declare
  range_start      date := to_date('03.06.2020', 'dd.mm.yyyy');
  range_start_date number := to_number(to_char(to_date('03.06.2020',
                                                       'dd.mm.yyyy'),
                                               'j'));
  range_end_date   number := to_number(to_char(to_date('20.08.2021',
                                                       'dd.mm.yyyy'),
                                               'j'));

  current_date   date;
  begin_date     date;
  begin_date_hol date;
  iCounter       integer := 0;
  isPostponent   integer := 0;

  FIRST_QUARTER_ID    constant number := 4;
  SECOND_QUARTER_ID   constant number := 8;
  THIRD_QUARTER_ID    constant number := 12;
  FOURTH_QUARTER_ID   constant number := 16;
  FIRST_HALF_YEAR_ID  constant number := 17;
  SECOND_HALF_YEAR_ID constant number := 18;
  YEAR_ID             constant number := 0;

  JANUARY   constant number := 1;
  FEBRUARY  constant number := 2;
  MARCH     constant number := 3;
  APRIL     constant number := 4;
  MAY       constant number := 5;
  JUNE      constant number := 6;
  JULY      constant number := 7;
  AUGUST    constant number := 8;
  SEPTEMBER constant number := 9;
  OCTOBER   constant number := 10;
  NOVEMBER  constant number := 11;
  DECEMBER  constant number := 12;

  FNO_200_00 constant varchar2(6) := '200.00';
  FNO_910_00 constant varchar2(6) := '910.00';
  FNO_912_00 constant varchar2(6) := '912.00';
  FNO_920_00 constant varchar2(6) := '920.00';

  PATENT                 constant number := 1;
  SINGLE_LAND_TAX        constant number := 3;
  AGRICULTURAL_PRODUCERS constant number := 11;
  GENERAL_ORDER          constant number := -1;
  SIMPLIFIED_DECLARATION constant number := 2;
  FIXED_DEDUCTION        constant number := 12;

  PRIMARY     constant number := 1;
  REGULAR     constant number := 2;
  LIQUIDATION constant number := 3;

  type postponement_map_type is record(
    rnn      varchar2(12),
    deadline date);
  postponement_map postponement_map_type;
  type postponement_table_type is table of postponement_map_type;
  postponement_table postponement_table_type := postponement_table_type();

  type specTaxModes_type is table of SPECIAL_TAX_MODE%rowtype;
  specTaxModes    specTaxModes_type := specTaxModes_type();
  npSpecTaxModes1 specTaxModes_type;
  npSpecTaxModes2 specTaxModes_type;

  period_first_date date;
  period_last_date  date;
  final_date        date;
  isStmEqual        boolean;
  counter           integer := 0;
  isTest            varchar2(10) := 'prod'; -- test
  tp_rnn            VARCHAR2(255 CHAR);
  tp_bin            VARCHAR2(255 CHAR);
  tp_id             number;

  procedure write_log(p_key varchar2, p_text varchar2) is
    pragma autonomous_transaction;
  begin
    insert into JAVA_APP_SIMPLE_LOG
      (ID, KEY, CLASS_NAME, TEXT)
    values
      (SEQ$JAVA_APP_LOG.NEXTVAL * 50, p_key, 'FNOSuspension', p_text);
    commit;
  end write_log;

  function calculateDateRegardHolidays(start_date in date) return date is
    day_of_week integer;
    day_off_row DAY_OFF%rowtype;
    result_date date := trunc(start_date);
    i           integer := 0;
  begin
    while i < 30 loop
      begin
        select do.*
          into day_off_row
          from DAY_OFF do
         where trunc(do.dat) = trunc(result_date);
      
        if day_off_row.type_date = 2 then
          result_date := result_date + 1;
        else
          exit;
        end if;
      exception
        when others then
          day_of_week := to_char(result_date, 'D');
          if day_of_week = 6 or day_of_week = 7 then
            result_date := result_date + 1;
          else
            exit;
          end if;
      end;
    
      i := i + 1;
    end loop;
    return result_date;
  end calculateDateRegardHolidays;

  procedure fillTmpSuspension is
    counterSusp   integer := 0;
    tp_bin_local  VARCHAR2(255 CHAR);
    tp_type_local VARCHAR2(255 CHAR);
    tp_id_local   number;
  begin
    delete from TMP_SUSPENSION;
    for tmpSusp in (select rnn, resumption_begin_date, id_suspension
                      from (select rnn,
                                   resumption_begin_date,
                                   id_suspension,
                                   row_number() over(partition by rnn order by resumption_begin_date desc) as rn
                              from REQ_INIS_SUSPENSION
                             where is_active = 1)
                     where rn = 1
                       and resumption_begin_date > range_start - 61) loop
      tp_rnn := tmpSusp.rnn;
    
      select tp.id, tp.bin, tp.tax_payer_type
        into tp_id_local, tp_bin_local, tp_type_local
        from TAX_PAYER tp
       where tp.rnn = tp_rnn;
    
      if tp_type_local = 'INDIVIDUAL_BUSINESSMAN' then
        insert into TMP_SUSPENSION
          (RNN, resumption_begin_date, Id_Suspension, tp_id, tp_bin)
        values
          (tmpSusp.Rnn,
           tmpSusp.Resumption_Begin_Date,
           tmpSusp.Id_Suspension,
           tp_id_local,
           tp_bin_local);
        counterSusp := counterSusp + 1;
        if counterSusp = 1000 then
          counterSusp := 0;
          commit;
        end if;
      end if;
    end loop;
    commit;
  end;

  procedure getPeriods is
    month                   number := to_number(to_char(current_date, 'mm'));
    year                    number := to_number(to_char(current_date,
                                                        'yyyy'));
    submissiondateHoliday   date;
    prolongationdateHoliday date;
  begin
    delete from SNO#EISI.TMP_PERIODS;
    if month >= JANUARY and month <= MARCH then
      insert into SNO#EISI.TMP_PERIODS
        (periodid, year, lastmonth, fnocode)
      values
        (THIRD_QUARTER_ID, year - 1, SEPTEMBER, FNO_200_00);
    elsif month >= APRIL and month <= JUNE then
      insert into SNO#EISI.TMP_PERIODS
        (periodid, year, lastmonth, fnocode)
      values
        (FOURTH_QUARTER_ID, year - 1, DECEMBER, FNO_200_00);
      insert into SNO#EISI.TMP_PERIODS
        (periodid, year, lastmonth, fnocode)
      values
        (SECOND_HALF_YEAR_ID, year - 1, DECEMBER, FNO_910_00);
      insert into SNO#EISI.TMP_PERIODS
        (periodid, year, lastmonth, fnocode)
      values
        (YEAR_ID, year - 1, DECEMBER, FNO_912_00);
      insert into SNO#EISI.TMP_PERIODS
        (periodid, year, lastmonth, fnocode)
      values
        (YEAR_ID, year - 1, DECEMBER, FNO_920_00);
    elsif month >= JULY and month <= SEPTEMBER then
      insert into SNO#EISI.TMP_PERIODS
        (periodid, year, lastmonth, fnocode)
      values
        (FIRST_QUARTER_ID, year, MARCH, FNO_200_00);
    elsif month >= OCTOBER and month <= DECEMBER then
      insert into SNO#EISI.TMP_PERIODS
        (periodid, year, lastmonth, fnocode)
      values
        (SECOND_QUARTER_ID, year, JUNE, FNO_200_00);
      insert into SNO#EISI.TMP_PERIODS
        (periodid, year, lastmonth, fnocode)
      values
        (FIRST_HALF_YEAR_ID, year, JUNE, FNO_910_00);
    end if;
  
    update SNO#EISI.TMP_PERIODS p
       set p.submissiondate = add_months(to_date('01.' || p.lastmonth || '.' ||
                                                 p.year,
                                                 'dd.mm.yyyy'),
                                         2) + 14
     where p.fnocode = FNO_200_00
        or p.fnocode = FNO_910_00;
  
    update SNO#EISI.TMP_PERIODS p
       set p.prolongationdate = p.submissiondate + 30
     where p.fnocode = FNO_200_00;
  
    update SNO#EISI.TMP_PERIODS p
       set p.submissiondate = add_months(to_date('31.03.' || p.year,
                                                 'dd.mm.yyyy'),
                                         12)
     where p.fnocode = FNO_912_00
        or p.fnocode = FNO_920_00;
  
    update SNO#EISI.TMP_PERIODS p
       set p.prolongationdate = p.submissiondate + 15
     where p.fnocode = FNO_912_00
        or p.fnocode = FNO_910_00
        or p.fnocode = FNO_920_00;
  
    for periods in (select * from SNO#EISI.TMP_PERIODS) loop
    
      submissiondateHoliday   := calculateDateRegardHolidays(periods.submissiondate);
      prolongationdateHoliday := calculateDateRegardHolidays(periods.prolongationdate);
    
      update SNO#EISI.TMP_PERIODS p
         set p.submissiondate   = submissiondateHoliday,
             p.prolongationdate = prolongationdateHoliday
       where p.fnocode = periods.fnocode;
    
    end loop;
  
  end getPeriods;

  function getPeriodFirstDate(year in number, periodid in number) return date is
    year_begin  date := trunc(to_date(year, 'YYYY'), 'YYYY');
    result_date date := trunc(current_date);
  begin
    if periodid = FIRST_QUARTER_ID or periodid = FIRST_HALF_YEAR_ID or
       periodid = YEAR_ID then
      result_date := add_months(year_begin, JANUARY);
    elsif periodid = SECOND_QUARTER_ID then
      result_date := add_months(year_begin, APRIL);
    elsif periodid = THIRD_QUARTER_ID or periodid = SECOND_HALF_YEAR_ID then
      result_date := add_months(year_begin, JULY);
    elsif periodid = FOURTH_QUARTER_ID then
      result_date := add_months(year_begin, OCTOBER);
    end if;
    result_date := add_months(result_date, -1);
    return result_date;
  end getPeriodFirstDate;

  function getPeriodLastDate(year in number, periodid in number) return date is
    year_begin  date := trunc(to_date(year, 'YYYY'), 'YYYY');
    result_date date := trunc(current_date);
  begin
    if periodid = FIRST_QUARTER_ID then
      result_date := add_months(year_begin, MARCH);
    elsif periodid = SECOND_QUARTER_ID or periodid = FIRST_HALF_YEAR_ID then
      result_date := add_months(year_begin, JUNE);
    elsif periodid = THIRD_QUARTER_ID then
      result_date := add_months(year_begin, SEPTEMBER);
    elsif periodid = FOURTH_QUARTER_ID or periodid = SECOND_HALF_YEAR_ID or
          periodid = YEAR_ID then
      result_date := add_months(year_begin, DECEMBER);
    end if;
    result_date := result_date - 1;
    return result_date;
  end getPeriodLastDate;

  function getPostponementFinalDate(rnn in varchar2) return date is
    result_date date := null;
  begin
    if postponement_table.count > 0 then
      for i in postponement_table.first .. postponement_table.last loop
        if postponement_table(i).rnn = rnn then
          result_date := postponement_table(i).deadline;
          exit;
        end if;
      end loop;
    end if;
    return result_date;
  end getPostponementFinalDate;

  procedure addSpecTaxMode(specTaxMode in number) is
  begin
    specTaxModes.extend;
    specTaxModes(specTaxModes.count).code := specTaxMode;
  end addSpecTaxMode;

  procedure getSpecTaxModeForPeriod(fno_code in varchar2) is
  begin
    specTaxModes.delete;
    if fno_code = FNO_200_00 then
      addSpecTaxMode(AGRICULTURAL_PRODUCERS);
      addSpecTaxMode(GENERAL_ORDER);
    elsif fno_code = FNO_910_00 then
      addSpecTaxMode(SIMPLIFIED_DECLARATION);
    elsif fno_code = FNO_912_00 then
      addSpecTaxMode(FIXED_DEDUCTION);
    elsif fno_code = FNO_920_00 then
      addSpecTaxMode(SINGLE_LAND_TAX);
    end if;
  end getSpecTaxModeForPeriod;

  function getNpSpecTaxMode(tp_id in number, stm_date in date)
    return specTaxModes_type is
    npSpecTaxModesLocal1 specTaxModes_type;
    npSpecTaxModesLocal2 specTaxModes_type;
    npSpecTaxModesLocal3 specTaxModes_type;
    cursor l_cursor(p_tp_id number, p_stm_date date, p_action_type number) is
      select t.*
        from SPECIAL_TAX_MODE t
       where t.tp_id = p_tp_id
         and t.action_date <= p_stm_date
         and t.action_type = p_action_type
         and t.code is not null
         and t.is_deleted = 0
       order by t.action_date desc;
  begin
    npSpecTaxModesLocal1 := specTaxModes_type();
    for npSpecTaxModesRows in l_cursor(tp_id, stm_date, 1) loop
      npSpecTaxModesLocal1.extend;
      npSpecTaxModesLocal1(npSpecTaxModesLocal1.count).code := npSpecTaxModesRows.code;
      npSpecTaxModesLocal1(npSpecTaxModesLocal1.count).action_date := npSpecTaxModesRows.action_date;
      npSpecTaxModesLocal1(npSpecTaxModesLocal1.count).IS_DELETED := 0;
    end loop;
  
    npSpecTaxModesLocal2 := specTaxModes_type();
    for npSpecTaxModesRows in l_cursor(tp_id, stm_date, 0) loop
      npSpecTaxModesLocal2.extend;
      npSpecTaxModesLocal2(npSpecTaxModesLocal2.count).code := npSpecTaxModesRows.code;
      npSpecTaxModesLocal2(npSpecTaxModesLocal2.count).action_date := npSpecTaxModesRows.action_date;
    end loop;
  
    if npSpecTaxModesLocal2.count > 0 then
      for j in npSpecTaxModesLocal2.first .. npSpecTaxModesLocal2.last loop
        if (npSpecTaxModesLocal1.count > 0) then
          for i in npSpecTaxModesLocal1.first .. npSpecTaxModesLocal1.last loop
            if npSpecTaxModesLocal1(i).IS_DELETED = 0 then
              if npSpecTaxModesLocal1(i)
               .code = npSpecTaxModesLocal2(j).code and npSpecTaxModesLocal1(i)
                 .action_date <= npSpecTaxModesLocal2(j).action_date then
                npSpecTaxModesLocal1(i).IS_DELETED := 1;
              end if;
            end if;
          end loop;
        end if;
      end loop;
    end if;
  
    npSpecTaxModesLocal3 := specTaxModes_type();
    if (npSpecTaxModesLocal1.count > 0) then
      for i in npSpecTaxModesLocal1.first .. npSpecTaxModesLocal1.last loop
        if npSpecTaxModesLocal1(i).IS_DELETED = 0 then
          npSpecTaxModesLocal3.extend;
          npSpecTaxModesLocal3(npSpecTaxModesLocal3.count).code := npSpecTaxModesLocal1(i).code;
          npSpecTaxModesLocal3(npSpecTaxModesLocal3.count).action_date := npSpecTaxModesLocal1(i)
                                                                          .action_date;
        end if;
      end loop;
    end if;
  
    return npSpecTaxModesLocal3;
  end getNpSpecTaxMode;

begin
  if isTest = 'prod' then
    delete from tmp_patent_suspension;
    delete from java_app_simple_log t where t.class_name = 'FNOSuspension';
    commit;
  end if;

  dbms_output.put_line('start_time: ' || CURRENT_TIMESTAMP);
  write_log('start_time', CURRENT_TIMESTAMP);

  fillTmpSuspension;

  for dateCursor in range_start_date .. range_end_date loop
    current_date := to_date(dateCursor, 'j');
  
    begin_date     := trunc(current_date) - 61;
    begin_date_hol := begin_date;
    begin_date     := calculateDateRegardHolidays(begin_date);
  
    if (begin_date_hol = begin_date) then
    
      getPeriods;
    
      for periods in (select * from SNO#EISI.TMP_PERIODS) loop
        if (begin_date - periods.prolongationdate = 0) or
           (begin_date - periods.submissiondate = 0) then
        
          write_log('current_date', current_date);
          write_log('begin_date', begin_date);
          write_log('period',
                    periods.year || ' ' || periods.lastmonth || ' ' ||
                    periods.fnocode);
        
          postponement_table.delete;
          for postponements in (select distinct p.*
                                  from POSTPONEMENTS p
                                 where p.fno_code = periods.fnocode
                                   and p.fno_report_year = periods.year
                                   and p.fno_period_id = periods.periodid
                                   and p.status = 1) loop
            postponement_table.extend;
            select doc.payer_rnn
              into postponement_map.rnn
              from document doc
             where doc.id = postponements.fno_doc_id;
            postponement_map.deadline := calculateDateRegardHolidays(postponements.submission_deadline_corr);
            postponement_table(postponement_table.count) := postponement_map;
          end loop;
        
          period_first_date := getPeriodFirstDate(periods.year,
                                                  periods.periodid);
          period_last_date  := getPeriodLastDate(periods.year,
                                                 periods.periodid);
          write_log('period_first_date', period_first_date);
          write_log('period_last_date', period_last_date);
        
          for r_susp in (select rnn,
                                resumption_begin_date,
                                id_suspension,
                                tp_id,
                                tp_bin
                           from TMP_SUSPENSION
                          where resumption_begin_date >= period_first_date
                            and resumption_begin_date <= period_last_date) loop
          
            tp_rnn := r_susp.rnn;
            tp_id  := r_susp.tp_id;
          
            isPostponent := 0;
            final_date   := getPostponementFinalDate(tp_rnn);
            if final_date is null then
              final_date := periods.submissiondate;
            end if;
            if begin_date - final_date <> 0 then
              isPostponent := 1;
            end if;
          
            if isPostponent = 0 then
              isStmEqual      := false;
              npSpecTaxModes1 := getNpSpecTaxMode(tp_id,
                                                  r_susp.resumption_begin_date);
              npSpecTaxModes2 := getNpSpecTaxMode(tp_id, trunc(sysdate));
              getSpecTaxModeForPeriod(periods.fnocode);
            
              if npSpecTaxModes1.count = 0 and npSpecTaxModes2.count = 0 then
                for i in specTaxModes.first .. specTaxModes.last loop
                  if specTaxModes(i).code = GENERAL_ORDER then
                    isStmEqual := true;
                  end if;
                end loop;
              end if;
            
              if npSpecTaxModes1.count > 1 or npSpecTaxModes2.count > 1 then
                isStmEqual := false;
              end if;
            
              if npSpecTaxModes1.count = 1 and npSpecTaxModes2.count = 1 then
                if (npSpecTaxModes1(npSpecTaxModes1.first)
                   .code = npSpecTaxModes2(npSpecTaxModes2.first).code) then
                  if specTaxModes.count > 0 then
                    for i in specTaxModes.first .. specTaxModes.last loop
                      if specTaxModes(i)
                       .code = npSpecTaxModes1(npSpecTaxModes1.first).code then
                        isStmEqual := true;
                      end if;
                    end loop;
                  end if;
                end if;
              end if;
            
              if isStmEqual then
                counter := 0;
                select count(doc.id)
                  into counter
                  from DOCUMENT doc
                 inner join TAB_DOC_VERSION ver
                    on doc.doc_version_id = ver.id
                 inner join TAB_DOC tab
                    on ver.doc_id = tab.id
                 where doc.payer_rnn = tp_rnn
                   and doc.period_id = periods.periodid
                   and doc.report_year = periods.year
                   and tab.code = periods.fnocode
                   and doc.type_id in (PRIMARY, REGULAR, LIQUIDATION)
                   and doc.doc_status_id in (4,
                                             6,
                                             7,
                                             8,
                                             9,
                                             23,
                                             25,
                                             28,
                                             29,
                                             30,
                                             33,
                                             34,
                                             40,
                                             44,
                                             305);
              
                if counter = 0 then
                  select count(doc.id)
                    into counter
                    from DOCUMENT doc
                   inner join TAB_DOC_VERSION ver
                      on doc.doc_version_id = ver.id
                   inner join TAB_DOC tab
                      on ver.doc_id = tab.id
                   where doc.payer_rnn = tp_rnn
                     and doc.submit_date >= periods.submissiondate
                     and tab.code = periods.fnocode
                     and doc.type_id in (PRIMARY, REGULAR, LIQUIDATION)
                     and doc.doc_status_id in (4,
                                               6,
                                               7,
                                               8,
                                               9,
                                               23,
                                               25,
                                               28,
                                               29,
                                               30,
                                               33,
                                               34,
                                               40,
                                               44,
                                               305);
                  if counter = 0 then
                    if isTest = 'test' then
                      insert into tmp_patent_suspension
                      values
                        (r_susp.rnn,
                         null,
                         trunc(r_susp.resumption_begin_date - 1));
                    else
                      begin
                        insert into REQ_INIS_ABSENT_PROLONG
                        values
                          (SEQ$REQ_INIS_A_PROLONGATION.NEXTVAL,
                           LOWER(REGEXP_REPLACE(SYS_GUID(),
                                                '(.{8})(.{4})(.{4})(.{4})(.{12})',
                                                '\1-\2-\3-\4-\5')),
                           tp_rnn,
                           r_susp.tp_bin,
                           null,
                           trunc(r_susp.resumption_begin_date - 1),
                           r_susp.id_suspension,
                           trunc(sysdate),
                           null,
                           'IS_TOTALLY_NEW');
                      exception
                        when others then
                          write_log('duplicate', tp_rnn);
                      end;
                    end if;
                    iCounter := iCounter + 1;
                    if (iCounter = 1000) then
                      iCounter := 0;
                      commit;
                    end if;
                  end if;
                end if;
              end if;
            end if;
          end loop;
        
        end if;
      end loop;
    end if;
  end loop;

  commit;
  dbms_output.put_line('end_time: ' || CURRENT_TIMESTAMP);
  write_log('end_time', CURRENT_TIMESTAMP);
end;
