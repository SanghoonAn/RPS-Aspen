package aspen;

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
import com.follett.fsc.core.k12.beans.SchoolCalendar;
import com.follett.fsc.core.k12.beans.SchoolCalendarDate;
import com.follett.fsc.core.k12.beans.X2BaseBean;
import com.follett.fsc.core.k12.tools.reports.ReportDataGrid;
import com.follett.fsc.core.k12.tools.reports.ReportJavaSourceNet;
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
	 
	private static final String							FIELD_SORT						= "SORT COLUMN";
	
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
	private Map<String, Collection<PairedEnrollment>>m_schoolMemberships;
	private Map<String, Collection<SchoolCalendarDate>> m_calendarDays; 
	private Map<String,HashMap<String, SchoolCalendar>> m_schoolCalendars  ;
	
	@Override
	protected Object gatherData() throws Exception {
		 ReportDataGrid grid = new ReportDataGrid();
		
		 
		m_startDate =(PlainDate)getParameter(START_DATE);
		m_endDate =(PlainDate)getParameter(END_DATE);
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
		addParameter(END_DATE,m_endDate );
	//	loadCalendarMaps();
		LoadEnrollments();
		loadStudents();
		
		/// populate the School memberships for membership counts.
		populateEnrollments();
		
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
		criteria.addNotEqualTo(StudentEnrollment.REL_STUDENT + PATH_DELIMITER + SisStudent.COL_GRADE_LEVEL, GRADE_PK);
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
		StuCriteria.addNotEqualTo(  SisStudent.COL_GRADE_LEVEL, GRADE_PK);
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
						AllAdmit.add(enrollment.getStudentOid());
						AllTotal.add(enrollment.getStudentOid());
						}
					}
					else if (enrollment.getEnrollmentType().equals(CON_WITHDRAW) && !excludeWD.contains(enrollment.getEnrollmentCode() ))
					{
						AllWithdraw.add(enrollment.getStudentOid());
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
			
			grid.set(FIELD_ALL_ADMIT, AllAdmit.size());
			grid.set(FIELD_ALL_WITHDRAW,AllWithdraw.size());
			grid.set(FIELD_ALL_TOTAL ,AllTotal.size());
			grid.set(FIELD_DIS_ADMIT, DisAdmit.size());
			grid.set(FIELD_DIS_WITHDRAW, DisWithdraw.size());
			grid.set(FIELD_DIS_TOTAL, DisTotal.size());
			grid.set(FIELD_ELL_ADMIT, ELLAdmit.size());
			grid.set(FIELD_ELL_WITHDRAW, ELLWithdraw.size());
			grid.set(FIELD_ELL_TOTAL, ELLTotal.size());
			grid.set(FIELD_HOM_ADMIT, HomAdmit.size());
			grid.set(FIELD_HOM_WITHDRAW, HomWithdraw.size());
			grid.set(FIELD_HOM_TOTAL, HomTotal.size());
			
			int AllEnr =0; 
			int ELLEnr = 0; 
			int DisEnr = 0; 
			int HomEnr = 0; 
			
			Collection<String> AllStudents = new HashSet<String>();
			Collection<String> ELLStudents = new HashSet<String>();
			Collection<String> DisStudents = new HashSet<String>();
			Collection<String> HomStudents = new HashSet<String>();
			
			Collection<PairedEnrollment> PEnrollments = m_schoolMemberships.get(schoolOid);
	 
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
				grid.set(FIELD_ALL_ENR,Field10B);
				}
			}
			else if (School.getFieldA010() != null && !School.getFieldA010().isEmpty())
			{
				int Field10 = Integer.parseInt(  School.getFieldA010());
				BigDecimal Field10B  =  new BigDecimal(Field10); 
			grid.set(FIELD_ALL_ENR,Field10B);
			}		 
			 
		}
		}
		} 
	}
	
	
 
	
	private HashMap<String,Collection<PairedEnrollment>> populateEnrollments()
	{
		HashMap<String,Collection<PairedEnrollment>> results = new HashMap<String,Collection<PairedEnrollment>>();
		m_schoolMemberships = new HashMap<String, Collection<PairedEnrollment >>();
		X2Criteria m_enrollmentCriteria = new X2Criteria();  
		m_enrollmentCriteria.addNotEqualTo(StudentEnrollment.COL_ENROLLMENT_TYPE, "Y");
		m_enrollmentCriteria.addIn(StudentEnrollment.COL_SCHOOL_OID, m_schoolSub);
		QueryByCriteria query = new QueryByCriteria(StudentEnrollment.class, m_enrollmentCriteria);
		query.addOrderByAscending( StudentEnrollment.COL_STUDENT_OID  );
		query.addOrderByAscending(StudentEnrollment.COL_ENROLLMENT_DATE );
		query.addOrderByAscending(StudentEnrollment.COL_TIMESTAMP );
		query.setDistinct(true);

		QueryIterator enrollmentRecordsIterator = getBroker().getIteratorByQuery(query);
		String LastStdOid = null;
		Collection<PairedEnrollment> collection = new HashSet();
		PairedEnrollment Pair = null;
		while (enrollmentRecordsIterator.hasNext())
		{ 
		
			StudentEnrollment enrollmentRecord = (StudentEnrollment) enrollmentRecordsIterator.next();
			if (enrollmentRecord.getSchoolOid() !=null && enrollmentRecord.getStudentOid()!=null && enrollmentRecord.getEnrollmentDate() != null )
				{
			String studentOid = enrollmentRecord.getStudentOid();

			if (LastStdOid== null)  /// initialize loop
			{
				LastStdOid = studentOid;
			}

			if (!LastStdOid.equals(studentOid))  /// if new student then wrap up last student.
			{
				if (Pair != null )
				{	//Pair.membershipDays = getMembershipDays(Pair);
					collection.add(Pair);
					if (!m_schoolMemberships.containsKey(Pair.SchoolOid))
					{
						Collection<PairedEnrollment> temp = new HashSet();
						m_schoolMemberships.put(Pair.SchoolOid,  temp);
					}
					m_schoolMemberships.get(Pair.SchoolOid).add(Pair);

					Pair = null;
				}

				if (collection.size() >0)
				{
					results.put(LastStdOid, collection);
					collection = new HashSet();
				} 
			}


			/// if a Enrollment Record then Create a new Paired Enrollment Record.
			if (enrollmentRecord.getEnrollmentType() != null && enrollmentRecord.getEnrollmentType().equals("E") && enrollmentRecord.getEnrollmentDate() != null)
			{	 
				Pair = new PairedEnrollment(enrollmentRecord.getSchoolOid(), enrollmentRecord.getStudentOid(), enrollmentRecord.getEnrollmentDate());  
			}

			/// If the Withdraw Record
			if (enrollmentRecord.getEnrollmentType() != null && enrollmentRecord.getEnrollmentType().equals("W") && Pair != null )
			{
				//	logMessage ( "Withdraw Record "+ enrollmentRecord.getOid());
				if (Pair.SchoolOid.equals( enrollmentRecord.getSchoolOid()) ) // && enrollmentRecord.getEnrollmentDate().compareTo(m_YearStartDate) >0)
				{
					Pair.WithdrawDate = enrollmentRecord.getEnrollmentDate(); 
					//Pair.membershipDays = getMembershipDays(Pair);
					collection.add(Pair); 
					if (!m_schoolMemberships.containsKey(Pair.SchoolOid))
					{
						Collection<PairedEnrollment> temp = new HashSet();
						m_schoolMemberships.put(Pair.SchoolOid,  temp);
					}
					m_schoolMemberships.get(Pair.SchoolOid).add(Pair);
					Pair = null;					
				}				
			}	
			LastStdOid = studentOid;
		} 
		}
		if (Pair != null )
		{
			//Pair.membershipDays = getMembershipDays(Pair);
			collection.add(Pair); 
			if (!m_schoolMemberships.containsKey(Pair.SchoolOid))
			{
				Collection<PairedEnrollment> temp = new HashSet();
				m_schoolMemberships.put(Pair.SchoolOid,  temp);
			}
			m_schoolMemberships.get(Pair.SchoolOid).add(Pair);
			//	logMessage ("Pair " + Pair.SchoolOid + " Reg date " + Pair.RegistrationDate +" Added");
		}

		if (collection.size() >0)
		{
			results.put(LastStdOid, collection); 
		}
		
		enrollmentRecordsIterator.close(); 

		return results;
	}

	/**
	 * Loads the Calendar Days and School Calendars into two separate Maps.  Maps are used to determine the Schedule day of the run date in
	 * order to determine if the class in question met on the run date.
	 * 
	 * @param none
	 * 
	 * @return none;
	 */
	private void loadCalendarMaps()
	{    
		/* 
		 * Build Calendar Dates Map
		 */
		Criteria criteria = new Criteria(); 
		criteria.addGreaterOrEqualThan(SchoolCalendarDate.COL_DATE,m_startDate );
		criteria.addLessOrEqualThan(SchoolCalendarDate.COL_DATE,m_endDate );
		criteria.addEqualTo(SchoolCalendarDate.COL_IN_SESSION_INDICATOR, true);
		QueryByCriteria query = new QueryByCriteria(SchoolCalendarDate.class, criteria);   
		query.addOrderByAscending(SchoolCalendarDate.COL_SCHOOL_CALENDAR_OID);
		query.addOrderByAscending(SchoolCalendarDate.COL_DATE);
		query.setDistinct(true);
		m_calendarDays = getBroker().getGroupedCollectionByQuery(query, SchoolCalendarDate.COL_SCHOOL_CALENDAR_OID,600);

		/* 
		 * Build School Calendar ID Map
		 */

		criteria = new Criteria();
		criteria.addEqualTo( SchoolCalendar.COL_DISTRICT_CONTEXT_OID, m_currentYear.getOid());  
		query = new QueryByCriteria(SchoolCalendar.class, criteria);
		query.addOrderByDescending(SchoolCalendar.COL_CALENDAR_ID);	 	
		Collection <SchoolCalendar> Cals = getBroker().getCollectionByQuery(query); 
		m_schoolCalendars = getBroker().getNestedMapByQuery(query,SchoolCalendar.COL_SCHOOL_OID,   SchoolCalendar.COL_CALENDAR_ID,200, 5); 
	}
	
	public int getMembershipDays (   PairedEnrollment enr)
	{
		int DayCount = 0;
		if  (m_schoolCalendars.containsKey(enr.SchoolOid)   )
		{
			
			String key = m_schoolCalendars.get(enr.SchoolOid).values().iterator().next().getOid();
			Collection<SchoolCalendarDate> days =   m_calendarDays.get(key);
			if ( days != null)
			{
			for (SchoolCalendarDate cDate : days)
			{
				if(cDate.getDate().compareTo(m_startDate) >= 0 &&
						cDate.getDate().compareTo(m_endDate) < 0 &&
						cDate.getDate().compareTo(enr.RegistrationDate) >= 0  &&
						((enr.WithdrawDate != null && cDate.getDate().compareTo(enr.WithdrawDate) < 0) ||enr.WithdrawDate == null )  
						)
				{
					DayCount ++;
				}
			}
			}
		}	
		
		return DayCount;
	}
	
	public class PairedEnrollment
	{
		public PlainDate RegistrationDate;
		public String studentOid;
		public PlainDate WithdrawDate;
		public String SchoolOid; 
		public int membershipDays;


		public PairedEnrollment( String SOid, String stu, PlainDate RegDate)
		{
			SchoolOid = SOid;
			studentOid = stu;
			RegistrationDate = RegDate;
			WithdrawDate = null; 
			membershipDays= 0;
		}

	}
	
}