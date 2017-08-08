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
	 *	Report Field Students with Disability Admissions.
	 */	
	private static final String							FIELD_DIS_ADMIT					= "DisAdmit";
	
	/**
	 *	Report Field Students with Disability Withdrawals.
	 */	
	private static final String							FIELD_DIS_WITHDRAW				= "DisWithdraw";
	
	/**
	 *	Report Field LEP Students Admissions.
	 */	
	private static final String							FIELD_ELL_ADMIT					= "ELLAdmit";
	
	/**
	 *	Report Field LEP Students Withdrawals.
	 */	
	private static final String							FIELD_ELL_WITHDRAW				= "ELLWithdraw";
	
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
	 * 
	 *	Report Field Homeless Students Withdrawals.
	 */	
	private static final String							FIELD_SORT						= "SORT COLUMN";
	
	private final String[] EXCLUDE_CODES_EN={ "E099","E100","E110","E119","R115","R403","R099","FSCW","NoSho","W016","W115","W118","W119","W201","W212","W214",
										   "W217","W218","W219","W221","W222","W304","W305","W306","W307","W308","W309",
										   "W310","W312","W313","W314","W321","W402","W411","W503","W650","W730","W731",
										   "W732","W870","W880","W960","W961","W970"};
	
	private final String[] EXCLUDE_CODES_WD={  "E099","E100","E104","E105","E106","E107","E108","E109","E110","E111","E113","E119",
											"E120","E121","E203","R111","R115","R201","R212","R214","R216","R217","R218","R219",
											"R298","R302","R312","R402","R403","R415","R416","R417","R418","W119","W650","W730","W731","W732",};
	
	private PlainDate m_YearStartDate; 
	private PlainDate m_endDate;
	private PlainDate m_startDate;
	private Map <String, SisStudent> m_students;
	private Map <String, Collection <StudentEnrollment>> m_enrollments;
	 

	private SisSchool m_currentSchool;
	private SubQuery m_schoolSub;
	private Collection<SisSchool> m_schools;
	private DistrictSchoolYearContext m_currentYear;
	private Map<String, Collection<PairedEnrollment>>m_schoolMemberships;
	
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
		
		addParameter(START_DATE,m_startDate );
		addParameter(END_DATE,m_endDate );
		
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
		
		if (isSchoolContext()){
			subCriteria.addEqualTo(SisSchool.COL_OID, getSchool().getOid());
		}
		else if (m_currentSchool != null )
		{
			subCriteria.addEqualTo(SisSchool.COL_OID, m_currentSchool.getOid());
		} 
		else 
		{
		String queryString=  (String) getParameter(QUERY_BY);  
		
		if (queryString!= null && queryString.equals("##current"))
		{
			Criteria criteria = new Criteria();
			criteria.addEqualTo(SisSchool.COL_INACTIVE_INDICATOR, false);
			
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
		for( SisSchool School : m_schools)
		{
			String schoolOid = School.getOid(); 
			if (School != null)
			{
			grid.append(); 
			grid.set(FIELD_SCHOOL, School);
			grid.set(FIELD_SORT, School.getName());
			int AllAdmit =0;
			int AllWithdraw = 0;
			int ELLAdmit = 0;
			int ELLWithdraw = 0;
			int DisAdmit = 0;
			int DisWithdraw = 0;
			int HomAdmit = 0;
			int HomWithdraw = 0;
			
			Collection<StudentEnrollment> Enrollments = m_enrollments.get(schoolOid);
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
						AllAdmit = AllAdmit +1;
						}
					}
					else if (enrollment.getEnrollmentType().equals(CON_WITHDRAW) && !excludeWD.contains(enrollment.getEnrollmentCode() ))
					{
						AllWithdraw = AllWithdraw+1;
					}
					
					// LEP Student
					if (student.getFieldB026() != null && !student.getFieldB026().isEmpty() && !student.getFieldB026().equals("3"))
					 { 
						if (enrollment.getEnrollmentType().equals(CON_ADMIT) )
						{
							if ( !excludeER.contains(enrollment.getEnrollmentCode() ))
							{
							ELLAdmit = ELLAdmit +1;
							}
						}
						else if (enrollment.getEnrollmentType().equals(CON_WITHDRAW)&& !excludeWD.contains(enrollment.getEnrollmentCode() ))
						{
							ELLWithdraw = ELLWithdraw+1;
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
							DisAdmit = DisAdmit +1;
							}
						}
						else if (enrollment.getEnrollmentType().equals(CON_WITHDRAW )&& !excludeWD.contains(enrollment.getEnrollmentCode() ))
						{
							DisWithdraw = DisWithdraw+1;
						}
					 }
					// Homeless
					if (student.getFieldA097() != null && (student.getFieldA097().equals("Y") || student.getFieldA097().equals("1")) )
					 {
						if (enrollment.getEnrollmentType().equals(CON_ADMIT) )
						{
							if ( !excludeER.contains(enrollment.getEnrollmentCode() ))
							{
							HomAdmit = HomAdmit +1;
							}
						}
						else if (enrollment.getEnrollmentType().equals(CON_WITHDRAW) && !excludeWD.contains(enrollment.getEnrollmentCode() ))
						{
							HomWithdraw = HomWithdraw+1;
						}
					 }
					 }
				}				
			}
			}
			
			grid.set(FIELD_ALL_ADMIT, AllAdmit);
			grid.set(FIELD_ALL_WITHDRAW,AllWithdraw);
			grid.set(FIELD_DIS_ADMIT, DisAdmit);
			grid.set(FIELD_DIS_WITHDRAW, DisWithdraw);
			grid.set(FIELD_ELL_ADMIT, ELLAdmit);
			grid.set(FIELD_ELL_WITHDRAW, ELLWithdraw);
			grid.set(FIELD_HOM_ADMIT, HomAdmit);
			grid.set(FIELD_HOM_WITHDRAW, HomWithdraw);
			
			int AllEnr =0; 
			int ELLEnr = 0; 
			int DisEnr = 0; 
			int HomEnr = 0; 
			
			Collection<String> AllStudents = new HashSet<String>();
			Collection<String> ELLStudents = new HashSet<String>();
			Collection<String> DisStudents = new HashSet<String>();
			Collection<String> HomStudents = new HashSet<String>();
			
			Collection<PairedEnrollment> PEnrollments = m_schoolMemberships.get(schoolOid);
			if (PEnrollments !=null && !PEnrollments.isEmpty())
			{
			for (PairedEnrollment enrollment: PEnrollments)
			{
				if (enrollment.studentOid != null &&  !enrollment.studentOid.isEmpty()   &&
					(	(enrollment.RegistrationDate != null && enrollment.RegistrationDate.compareTo(m_endDate) <=0) && 
					   ( enrollment.WithdrawDate == null || enrollment.WithdrawDate.compareTo(m_startDate) >=0 )) 
						)
				{
				
					SisStudent student = null;
					if (m_students.containsKey(enrollment.studentOid )) 
							{
								student = m_students.get(enrollment.studentOid);
							}
					else
					{
						student = (SisStudent) getBroker().getBeanByOid(SisStudent.class, enrollment.studentOid);
					}
				if (student.getFieldA039()==null || student.getFieldA039().equals("0"))	
				{
					AllEnr = AllEnr +1;
					 AllStudents.add(student.getOid());
					
					// LEP Student
					if (student.getFieldB026() != null && !student.getFieldB026().isEmpty() && !student.getFieldB026().equals("3"))
					 {  
						ELLEnr = ELLEnr +1;
						ELLStudents.add(student.getOid());
					 }
					// Students with Disabilities 
					if (student.getSpedStatusCode()  != null &&  student.getSpedStatusCode().equals(STATUS_ACTIVE) )  
					 {  
						DisEnr = DisEnr+1;
						DisStudents.add(student.getOid());
					 }
					// Homeless
					if (student.getFieldA097() != null && (student.getFieldA097().equals("Y") || student.getFieldA097().equals("1")) )
					 { 
						HomEnr = HomEnr+1; 
						HomStudents.add(student.getOid());
					 }
					 }
				}				
			}
			}
			
			grid.set(FIELD_ALL_ENR, new BigDecimal(AllStudents.size())); 
			grid.set(FIELD_DIS_ENR, new BigDecimal(DisStudents.size())); 
			grid.set(FIELD_ELL_ENR, new BigDecimal(ELLStudents.size())); 
			grid.set(FIELD_HOM_ENR, new BigDecimal(HomStudents.size())); 
			
			
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
				{
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

	public class PairedEnrollment
	{
		public PlainDate RegistrationDate;
		public String studentOid;
		public PlainDate WithdrawDate;
		public String SchoolOid; 


		public PairedEnrollment( String SOid, String stu, PlainDate RegDate)
		{
			SchoolOid = SOid;
			studentOid = stu;
			RegistrationDate = RegDate;
			WithdrawDate = null; 
		}

	}
}
