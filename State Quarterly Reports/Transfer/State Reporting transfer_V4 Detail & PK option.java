 
	package aspen;

	import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
	import java.util.ArrayList;
	import java.util.Arrays;
	import java.util.Collection;
	import java.util.HashMap;
	import java.util.HashSet;
	import java.util.List;
	import java.util.Map;

	import org.apache.ojb.broker.query.Criteria;
	import org.apache.ojb.broker.query.QueryByCriteria;

	import com.follett.fsc.core.framework.persistence.SubQuery;
	import com.follett.fsc.core.framework.persistence.X2Criteria;
	import com.follett.fsc.core.k12.beans.DistrictSchoolYearContext;
	import com.follett.fsc.core.k12.beans.QueryIterator;
import com.follett.fsc.core.k12.beans.Report;
import com.follett.fsc.core.k12.beans.SchoolCalendar;
	import com.follett.fsc.core.k12.beans.SchoolCalendarDate;
	import com.follett.fsc.core.k12.beans.X2BaseBean;
	import com.follett.fsc.core.k12.tools.reports.ReportDataGrid;
	import com.follett.fsc.core.k12.tools.reports.ReportJavaSourceNet;
import com.follett.fsc.core.k12.tools.reports.ReportUtils;
import com.follett.fsc.core.k12.web.UserDataContainer;
	import com.x2dev.sis.model.beans.ConductIncident;
	import com.x2dev.sis.model.beans.SisSchool;
	import com.x2dev.sis.model.beans.SisStudent;
	import com.x2dev.sis.model.beans.StudentEnrollment;
	import com.x2dev.utils.types.PlainDate;
	 

	import static com.follett.fsc.core.k12.business.ModelProperty.PATH_DELIMITER;

	public class RPS_STATE_REPORTING_TRANSFER extends ReportJavaSourceNet
	{
		private static final String							STATUS_ACTIVE 					= "Active";
		private static final String							QUERY_BY 						= "queryBy";
		private static final String							GRADE_PK = "PK";
		private static final String							GRADE_IN = "IN";
		private static final String							GRADE_UN = "UN";
		
		/**
		 *	Enrollment type Admission.
		 */	
		private static final String							CON_ADMIT						= "E"; 
		
		/**
		 *	Enrollment Type Withdrawal.
		 */	
		private static final String							CON_WITHDRAW					= "W"; 
		
		/**
		 *	Start Date Parameter.
		 */	
		private static final String							START_DATE						= "StartDate"; 

		/**
		 *	End Date Parameter.
		 */	
		private static final String							END_DATE						= "EndDate"; 

		/**
		 *	Report Field School.
		 */	
		private static final String							FIELD_SCHOOL					= "School";
		
		/**
		 *	Report Field All Students Admissions.
		 */	
		private static final String							FIELD_ALL_ADMIT					= "AllAdmit";
		
		/**
		 *	Report Field All Students Withdrawals.
		 */	
		private static final String							FIELD_ALL_WITHDRAW				= "AllWithdraw";
		
		/**
		 *	Report Field All Students Transfer.
		 */	
		private static final String							FIELD_ALL_TOTAL					= "AllTotal";
		
		/**
		 *	Report Field Students with Disability Admissions.
		 */	
		private static final String							FIELD_DIS_ADMIT					= "DisAdmit";
		
		/**
		 *	Report Field Students with Disability Withdrawals.
		 */	
		private static final String							FIELD_DIS_WITHDRAW				= "DisWithdraw";
		
		/**
		 *	Report Field Students with Disability Transfer.
		 */	
		private static final String							FIELD_DIS_TOTAL					= "DisTotal";
		
		/**
		 *	Report Field LEP Students Admissions.
		 */	
		private static final String							FIELD_ELL_ADMIT					= "ELLAdmit";
		
		/**
		 *	Report Field LEP Students Withdrawals.
		 */	
		private static final String							FIELD_ELL_WITHDRAW				= "ELLWithdraw";

		/**
		 *	Report Field Total LEP Students Transfers.
		 */	
		private static final String							FIELD_ELL_TOTAL					= "ELLTotal";
		
		/**
		 *	Report Field Homeless Students Admissions.
		 */	
		private static final String							FIELD_HOM_ADMIT					= "HomAdmit";
		
		/**
		 *	Report Field All Enrollment.
		 */	
		private static final String							FIELD_ALL_ENR					= "AllEnr";
		
		/**
		 *	Report Field ELL Enrollment.
		 */	
		private static final String							FIELD_ELL_ENR					= "ELLEnr";
		
		/**
		 *	Report Field Disability Enrollment.
		 */	
		private static final String							FIELD_DIS_ENR					= "DisEnr";
		
		/**
		 *	Report Field Homeless Enrollment.
		 */	
		private static final String							FIELD_HOM_ENR					= "HomEnr";
		
		/**
		 *	Report Field Homeless Students Withdrawals.
		 */	
		private static final String							FIELD_HOM_WITHDRAW				= "HomWithdraw";
		
		/**
		 *	Report Field Total Homeless Students Transfer.
		 */	
		private static final String							FIELD_HOM_TOTAL					= "HomTotal";
		 
		
		/** Detail report fields
		 * 
		 */
		private static final String							FIELD_STUDENT_ID				= "StudentID";
		private static final String							FIELD_STUDENT_NAME				= "StudentName";
		private static final String							FIELD_TYPE						= "TransactionType";
		private static final String							FIELD_TRANS_DATE				= "TransferDate";
		private static final String							FIELD_CODE						= "Code";
		private static final String							FIELD_REASON					= "Reason";
		
		
		
		private static final String							FIELD_SORT						= "SORT COLUMN";
		private static final String							SUB_SUM							="SubSum";
		private static final String							SUB_DETAIL						="SubDetail";
		private static final String							GRID_SUM						="GridSum";
		private static final String							GRID_DETAIL						="GridDetail";
		
		private static final String 						SUB_ID_SUM						= "RPS_MOB_SUB1";
		private static final String 						SUB_ID_DETAIL					= "RPS_MOB_SUB2";
		
		
		private final String[] EXCLUDE_CODES_EN={ "E099","E100","E110","E119","R115","R403","R099","FSCW","NoSho","W016","W115","W118","W119","W201","W212","W214",
											   "W217","W218","W219","W221","W222","W304","W305","W306","W307","W308","W309",
											   "W310","W312","W313","W314","W321","W402","W411","W503","W650","W730","W731",
											   "W732","W870","W880","W960","W961","W970"};
		
		private final String[] EXCLUDE_CODES_WD={  "E099","E100","E104","E105","E106","E107","E108","E109","E110","E111","E113","E119",
												"E120","E121","E203","R111","R115","R201","R212","R214","R216","R217","R218","R219",
												"R298","R302","R312","R402","R403","R415","R416","R417","R418","W119","W650","W730","W731","W732",};
		
		private final String[] EXCLUDE_SCHOOLS ={ "0000","109","777","000","SUMES","SUMHS","SUMMS" };
		
		private final String[] AMELIA_SCHOOLS = {"307","471","S313","313"};
		private final String[] AMELIA_SCHOOLS_SUB = { "471","S313","313"};
		
		private PlainDate m_YearStartDate; 
		private PlainDate m_endDate;
		private PlainDate m_startDate;
		private Map <String, SisStudent> m_students;
		private Map <String, Collection <StudentEnrollment>> m_enrollments;
		 

		private SisSchool m_currentSchool;
		private SubQuery m_schoolSub;
		private Collection<SisSchool> m_schools;
		private Collection<SisSchool> m_AmSchools;
		private DistrictSchoolYearContext m_currentYear; 
		private Map<String, Collection<SchoolCalendarDate>> m_calendarDays; 
		private Map<String,HashMap<String, SchoolCalendar>> m_schoolCalendars  ;
		private Report SumReport;
		private Report DetailReport;
		private boolean IncludePK;
		
		@Override
		protected Object gatherData() throws Exception {
			 ReportDataGrid grid = new ReportDataGrid();
			
			 
			m_startDate =(PlainDate)getParameter(START_DATE);
			m_endDate =(PlainDate)getParameter(END_DATE);
			boolean detail = (boolean) getParameter("Detail");
			 IncludePK = (boolean) getParameter("PK");
			SumReport = ReportUtils.getReport(SUB_ID_SUM, getBroker());
			DetailReport = ReportUtils.getReport(SUB_ID_DETAIL, getBroker());
			
			getSchoolYear();
			m_YearStartDate = m_currentYear.getStartDate();
			if (m_endDate.compareTo(m_currentYear.getEndDate())>0 ) m_endDate = m_currentYear.getEndDate();
			
			m_schoolSub = getSchoolSubQuery();
			
			if (m_endDate.compareTo(m_startDate) < 0)
			{
				m_endDate = m_startDate;
			}
			getAmeliaSchools();
			addParameter(START_DATE,m_startDate );
			addParameter("Detail",detail );
			addParameter(END_DATE,m_endDate );
		 
			LoadEnrollments();
			loadStudents();
			 
			
			ProcessEnrollments(grid); 
			
			grid.beforeTop();
			grid.sort(FIELD_SORT, false);
			return grid;
		}
		
		
		public void getSchoolYear()
		{
			Criteria criteria = new Criteria();
			criteria.addGreaterOrEqualThan(DistrictSchoolYearContext.COL_END_DATE, m_startDate);
			criteria.addLessOrEqualThan(DistrictSchoolYearContext.COL_START_DATE, m_startDate);
			QueryByCriteria query =  new QueryByCriteria(DistrictSchoolYearContext.class, criteria);
			m_currentYear = (DistrictSchoolYearContext) getBroker().getBeanByQuery(query); 
		}

		
		
		public void getAmeliaSchools()
		{
			Criteria criteria = new Criteria();
			Collection<String> AmeliaSchools = new ArrayList<String>(Arrays.asList(AMELIA_SCHOOLS_SUB));
			criteria.addIn(SisSchool.COL_SCHOOL_ID , AmeliaSchools);
			
			QueryByCriteria query =  new QueryByCriteria(SisSchool.class, criteria);
			m_AmSchools =   getBroker().getCollectionByQuery(query);
		}

		
		
		private void LoadEnrollments()
		{

			Criteria criteria = new Criteria();
			criteria.addGreaterOrEqualThan(StudentEnrollment.COL_ENROLLMENT_DATE , m_YearStartDate);
			criteria.addGreaterOrEqualThan(StudentEnrollment.COL_ENROLLMENT_DATE , m_startDate);
			criteria.addLessOrEqualThan(StudentEnrollment.COL_ENROLLMENT_DATE , m_endDate); 
		//	criteria.addEqualTo(StudentEnrollment.COL_FIELD_A013, false);
		//	criteria.addEqualTo(StudentEnrollment.REL_STUDENT + PATH_DELIMITER + SisStudent.COL_FIELD_A032 , false);
			if (!IncludePK)	criteria.addNotEqualTo(StudentEnrollment.REL_STUDENT + PATH_DELIMITER + SisStudent.COL_GRADE_LEVEL, GRADE_PK);
			criteria.addNotEqualTo(StudentEnrollment.REL_STUDENT + PATH_DELIMITER + SisStudent.COL_GRADE_LEVEL, GRADE_UN);
			criteria.addNotEqualTo(StudentEnrollment.REL_STUDENT + PATH_DELIMITER + SisStudent.COL_GRADE_LEVEL, GRADE_IN);
			 
			criteria.addIn(StudentEnrollment.COL_SCHOOL_OID , m_schoolSub);
			 
			QueryByCriteria query = new QueryByCriteria(StudentEnrollment.class, criteria);

			m_enrollments =	getBroker().getGroupedCollectionByQuery(query  , StudentEnrollment.COL_SCHOOL_OID, 100); 
		}
	 
		
		
		/**
		 * Returns a sub query for the list of Schools to include in the query.
		 * 
		 * @param beanPath
		 * 
		 * @return SubQuery;
		 */
		private SubQuery getSchoolSubQuery( )
		{ 
	 
			Criteria subCriteria = new Criteria(); 
			Collection<String> excludeSchool = new ArrayList<String>(Arrays.asList(EXCLUDE_SCHOOLS));
			Collection<String> AmeliaSchools = new ArrayList<String>(Arrays.asList(AMELIA_SCHOOLS));
			
			if (isSchoolContext()){
				if (getSchool().getSchoolId().equals("307"))
				{
					subCriteria.addIn(SisSchool.COL_SCHOOL_ID, AmeliaSchools);
				}
				else
				{
				subCriteria.addEqualTo(SisSchool.COL_OID, getSchool().getOid());
				}
			}
			else if (m_currentSchool != null )
			{
				if (m_currentSchool.getSchoolId().equals("307"))
				{
					subCriteria.addIn(SisSchool.COL_SCHOOL_ID, AmeliaSchools);
				}
				else
				{
				subCriteria.addEqualTo(SisSchool.COL_OID, m_currentSchool.getOid());
				}
			} 
			else 
			{
			String queryString=  (String) getParameter(QUERY_BY);  
			
			if (queryString!= null && queryString.equals("##current"))
			{
				Criteria criteria = new Criteria();
				criteria.addEqualTo(SisSchool.COL_INACTIVE_INDICATOR, false);
				criteria.addNotIn(SisSchool.COL_SCHOOL_ID, excludeSchool);
				
				subCriteria.addAndCriteria(getCurrentCriteria());
				subCriteria.addAndCriteria(criteria);	
			 
			}
			else
			{
				subCriteria.addEqualTo(SisSchool.COL_INACTIVE_INDICATOR, false);
			}
			}

			SubQuery schoolSubQuery = new SubQuery(SisSchool.class, X2BaseBean.COL_OID, subCriteria); 
			
			QueryByCriteria  schoolQuery = new QueryByCriteria(SisSchool.class,   subCriteria); 
			m_schools = getBroker().getCollectionByQuery(schoolQuery);
			return schoolSubQuery; 
		}
		
		/**
		 * @see com.x2dev.sis.tools.ToolJavaSource#saveState(com.x2dev.sis.web.UserDataContainer)
		 */
		@Override
		protected void saveState(final UserDataContainer userData)
		{
			/*
			 * If we're in the context of a single student, print the report for just that student
			 */
			m_currentSchool = (SisSchool) userData.getCurrentRecord(SisSchool.class);
		}
		
		@SuppressWarnings("unchecked")
		private void loadStudents()
		{

			Criteria criteria = new Criteria();
			criteria.addGreaterOrEqualThan(StudentEnrollment.COL_ENROLLMENT_DATE , m_YearStartDate);
			criteria.addGreaterOrEqualThan(StudentEnrollment.COL_ENROLLMENT_DATE , m_startDate);
			criteria.addLessOrEqualThan(StudentEnrollment.COL_ENROLLMENT_DATE , m_endDate); 
		//	criteria.addEqualTo(StudentEnrollment.COL_FIELD_A013, false); 
			if (isSchoolContext())
			{
				criteria.addEqualTo(StudentEnrollment.COL_SCHOOL_OID , getSchool().getOid());
			}
			SubQuery subquery = new SubQuery(StudentEnrollment.class,StudentEnrollment.COL_STUDENT_OID , criteria);
			subquery.setDistinct(true); 
			 
			Criteria StuCriteria = new Criteria();
			StuCriteria.addIn(SisStudent.COL_OID, subquery);
			if (!IncludePK)	 StuCriteria.addNotEqualTo(  SisStudent.COL_GRADE_LEVEL, GRADE_PK);
			StuCriteria.addNotEqualTo(  SisStudent.COL_GRADE_LEVEL, GRADE_IN);
			StuCriteria.addNotEqualTo(  SisStudent.COL_GRADE_LEVEL, GRADE_UN);
			//StuCriteria.addEqualTo( SisStudent.COL_FIELD_A032 , false);
			QueryByCriteria stuQuery = new QueryByCriteria(SisStudent.class, StuCriteria);
			m_students = getBroker().getMapByQuery(stuQuery, SisStudent.COL_OID, 1000); 
		}

		
		private void ProcessEnrollments(final ReportDataGrid grid)
		{
			 
			Collection<String> excludeER  = new ArrayList<String>(Arrays.asList(EXCLUDE_CODES_EN));
			Collection<String> excludeWD = new ArrayList<String>(Arrays.asList(EXCLUDE_CODES_WD));
			Collection<String> AmeliaSchools = new ArrayList<String>(Arrays.asList(AMELIA_SCHOOLS_SUB));
			for( SisSchool School : m_schools)
			{
				if (!AmeliaSchools.contains(School.getSchoolId()))
				{
				String schoolOid = School.getOid(); 
				/*if  (m_schoolCalendars.containsKey(schoolOid)   )
				{
					
					String key = m_schoolCalendars.get(schoolOid).values().iterator().next().getOid();
				if (   m_calendarDays.containsKey(key) )
				{	*/
				 
				
				if (School != null)
				{
				ReportDataGrid SumGrid = new ReportDataGrid();
				ReportDataGrid DetailGrid = new ReportDataGrid();
					
				grid.append(); 
				grid.set(FIELD_SCHOOL, School);
				grid.set(FIELD_SORT, School.getName());
				Collection<String>  AllAdmit =new HashSet<String>();
				Collection<String>  AllWithdraw = new HashSet<String>();
				Collection<String>  AllTotal = new HashSet<String>();
				Collection<String>  ELLAdmit = new HashSet<String>();
				Collection<String>  ELLWithdraw = new HashSet<String>();
				Collection<String>  ELLTotal = new HashSet<String>();
				Collection<String>  DisAdmit = new HashSet<String>();
				Collection<String>  DisWithdraw = new HashSet<String>();
				Collection<String>  DisTotal = new HashSet<String>();
				Collection<String>  HomAdmit = new HashSet<String>();
				Collection<String>  HomWithdraw =  new HashSet<String>();
				Collection<String>  HomTotal = new HashSet<String>();
				
				Collection<StudentEnrollment> Enrollments = m_enrollments.get(schoolOid);
				
				// If Amelia Street then add Real school and 13 acres 
				if (School.getSchoolId().equals("307"))
				{
					for (SisSchool aSchool: m_AmSchools)
					{
					if (m_enrollments.containsKey(aSchool.getOid()))
					{
						Enrollments.addAll(m_enrollments.get(aSchool.getOid()));
					}
					}
				}

				if (Enrollments !=null && !Enrollments.isEmpty())
				{
				for (StudentEnrollment enrollment: Enrollments)
				{
					
					if (enrollment.getStudentOid() != null &&  !enrollment.getStudentOid().isEmpty() &&
							enrollment.getEnrollmentDate().compareTo(m_startDate) >=0 && enrollment.getEnrollmentDate().compareTo(m_endDate) <=0 
							&&(    enrollment.getFieldA013()== null ||    enrollment.getFieldA013().equals("0")  ))
					{
					
						SisStudent student = m_students.get(enrollment.getStudentOid()); 
					if (student.getFieldA039()==null || student.getFieldA039().equals("0"))	
					{
						if (enrollment.getEnrollmentType().equals(CON_ADMIT)  )
						{
							if ( !excludeER.contains(enrollment.getEnrollmentCode() ))
							{
								if (AllAdmit.add(enrollment.getStudentOid()))
								{
									DetailGrid.append();
									DetailGrid.set(FIELD_STUDENT_ID, student.getLocalId());
									DetailGrid.set(FIELD_STUDENT_NAME,student.getNameView());
									DetailGrid.set(FIELD_TYPE, "ENR");
									DetailGrid.set(FIELD_TRANS_DATE, enrollment.getEnrollmentDate());
									DetailGrid.set(FIELD_CODE, enrollment.getEnrollmentCode());
									DetailGrid.set(FIELD_REASON, enrollment.getReasonCode() );
									DetailGrid.set(FIELD_SORT,"ENR"+enrollment.getEnrollmentCode()+student.getLocalId() );
								}
							AllTotal.add(enrollment.getStudentOid());
							}
						}
						else if (enrollment.getEnrollmentType().equals(CON_WITHDRAW) && !excludeWD.contains(enrollment.getEnrollmentCode() ))
						{
							if (AllWithdraw.add(enrollment.getStudentOid() ))
							{
							DetailGrid.append(); 
							DetailGrid.set(FIELD_STUDENT_ID, student.getLocalId());
							DetailGrid.set(FIELD_STUDENT_NAME,student.getNameView());
							DetailGrid.set(FIELD_TYPE, "WTD");
							DetailGrid.set(FIELD_TRANS_DATE, enrollment.getEnrollmentDate());
							DetailGrid.set(FIELD_CODE, enrollment.getEnrollmentCode());
							DetailGrid.set(FIELD_REASON, enrollment.getReasonCode());
							DetailGrid.set(FIELD_SORT,"WTD"+enrollment.getEnrollmentCode()+student.getLocalId() );
							}
							AllTotal.add(enrollment.getStudentOid());
						}
						
						// LEP Student
						if (student.getFieldB026() != null && !student.getFieldB026().isEmpty() && !student.getFieldB026().equals("3"))
						 { 
							if (enrollment.getEnrollmentType().equals(CON_ADMIT) )
							{
								if ( !excludeER.contains(enrollment.getEnrollmentCode() ))
								{
								ELLAdmit.add(enrollment.getStudentOid());
								ELLTotal.add(enrollment.getStudentOid());
								}
							}
							else if (enrollment.getEnrollmentType().equals(CON_WITHDRAW)&& !excludeWD.contains(enrollment.getEnrollmentCode() ))
							{
								ELLWithdraw.add(enrollment.getStudentOid());
								ELLTotal.add(enrollment.getStudentOid());
							}
						 }
						// Students with Disabilities
					//	if (student.getFieldA002() != null && !student.getFieldA002().isEmpty())
						if (student.getSpedStatusCode()  != null &&  student.getSpedStatusCode().equals(STATUS_ACTIVE) )  
						 { 
							if (enrollment.getEnrollmentType().equals(CON_ADMIT) )
							{
								if ( !excludeER.contains(enrollment.getEnrollmentCode() ))
								{
								DisAdmit.add(enrollment.getStudentOid());
								DisTotal.add(enrollment.getStudentOid());
								}
							}
							else if (enrollment.getEnrollmentType().equals(CON_WITHDRAW )&& !excludeWD.contains(enrollment.getEnrollmentCode() ))
							{
								DisWithdraw.add(enrollment.getStudentOid());
								DisTotal.add(enrollment.getStudentOid());
							}
						 }
						// Homeless
						if (student.getFieldA097() != null && (student.getFieldA097().equals("Y") || student.getFieldA097().equals("1")) )
						 {
							if (enrollment.getEnrollmentType().equals(CON_ADMIT) )
							{
								if ( !excludeER.contains(enrollment.getEnrollmentCode() ))
								{
								HomAdmit.add(enrollment.getStudentOid());
								HomTotal.add(enrollment.getStudentOid());
								}
							}
							else if (enrollment.getEnrollmentType().equals(CON_WITHDRAW) && !excludeWD.contains(enrollment.getEnrollmentCode() ))
							{
								HomWithdraw.add(enrollment.getStudentOid());
								HomTotal.add(enrollment.getStudentOid());
							}
						 }
						 }
					}				
				}
				}
				SumGrid.append();

				SumGrid.set(FIELD_SCHOOL, School);
				SumGrid.set(FIELD_ALL_ADMIT, AllAdmit.size());
				SumGrid.set(FIELD_ALL_WITHDRAW,AllWithdraw.size());
				SumGrid.set(FIELD_ALL_TOTAL ,AllTotal.size());
				SumGrid.set(FIELD_DIS_ADMIT, DisAdmit.size());
				SumGrid.set(FIELD_DIS_WITHDRAW, DisWithdraw.size());
				SumGrid.set(FIELD_DIS_TOTAL, DisTotal.size());
				SumGrid.set(FIELD_ELL_ADMIT, ELLAdmit.size());
				SumGrid.set(FIELD_ELL_WITHDRAW, ELLWithdraw.size());
				SumGrid.set(FIELD_ELL_TOTAL, ELLTotal.size());
				SumGrid.set(FIELD_HOM_ADMIT, HomAdmit.size());
				SumGrid.set(FIELD_HOM_WITHDRAW, HomWithdraw.size());
				SumGrid.set(FIELD_HOM_TOTAL, HomTotal.size());
				
				int AllEnr =0; 
				int ELLEnr = 0; 
				int DisEnr = 0; 
				int HomEnr = 0; 
				
				Collection<String> AllStudents = new HashSet<String>();
				Collection<String> ELLStudents = new HashSet<String>();
				Collection<String> DisStudents = new HashSet<String>();
				Collection<String> HomStudents = new HashSet<String>();
				 
		 
				if (School.getSchoolId().equals("307"))
				{
					int Field10=0;
					if (School.getFieldA010() != null && !School.getFieldA010().isEmpty())
					{
						  Field10 = Integer.parseInt(  School.getFieldA010());
					
					
					for (SisSchool aSchool: m_AmSchools)
					{
					if (aSchool.getFieldA010() != null && !aSchool.getFieldA010().isEmpty())
					{
						int enr =  Integer.parseInt(  aSchool.getFieldA010());
						Field10 = Field10+enr;
					}
					}
					BigDecimal Field10B  =  new BigDecimal(Field10); 
					SumGrid.set(FIELD_ALL_ENR,Field10B);
					}
				}
				else if (School.getFieldA010() != null && !School.getFieldA010().isEmpty())
				{
					int Field10 = Integer.parseInt(  School.getFieldA010());
					BigDecimal Field10B  =  new BigDecimal(Field10); 
					SumGrid.set(FIELD_ALL_ENR,Field10B);
				}	
				SumGrid.beforeTop();
				DetailGrid.beforeTop();
				DetailGrid.sort(FIELD_SORT, false);
				grid.set(GRID_SUM, SumGrid);
				grid.set(GRID_DETAIL, DetailGrid); 
				grid.set(SUB_SUM,   new ByteArrayInputStream(SumReport.getCompiledFormat()));
				grid.set(SUB_DETAIL,   new ByteArrayInputStream(DetailReport.getCompiledFormat()));  
				 
			}
			}
			} 
		}
		
		
	  
		
	}
