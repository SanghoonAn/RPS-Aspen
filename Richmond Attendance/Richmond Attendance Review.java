	package aspen;
	
	import static com.follett.fsc.core.k12.business.ModelProperty.PATH_DELIMITER;
	
	import java.io.ByteArrayInputStream;
	import java.math.BigDecimal;
	import java.math.RoundingMode;
	import java.util.ArrayList;
	import java.util.Arrays;
	import java.util.Calendar;
	import java.util.Collection;
	import java.util.HashMap;
	import java.util.HashSet;
	import java.util.Iterator;
	import java.util.List;
	import java.util.Map;
	import java.util.Set;
	
	import org.apache.ojb.broker.query.Criteria;
	import org.apache.ojb.broker.query.QueryByCriteria;
	import org.apache.ojb.broker.query.ReportQueryByCriteria;
	import org.joda.time.LocalDate;
	import org.joda.time.Years;
	
	import com.follett.fsc.core.framework.persistence.SubQuery;
	import com.follett.fsc.core.framework.persistence.X2Criteria;
	import com.follett.fsc.core.k12.beans.DistrictSchoolYearContext;
	import com.follett.fsc.core.k12.beans.QueryIterator;
	import com.follett.fsc.core.k12.beans.Report;
	import com.follett.fsc.core.k12.beans.ReportQueryIterator;
	import com.follett.fsc.core.k12.beans.School;
	import com.follett.fsc.core.k12.beans.SchoolCalendar;
	import com.follett.fsc.core.k12.beans.SchoolCalendarDate;
	import com.follett.fsc.core.k12.beans.Selection;
	import com.follett.fsc.core.k12.beans.SelectionObject;
	import com.follett.fsc.core.k12.beans.SystemPreferenceDefinition;
	import com.follett.fsc.core.k12.beans.X2BaseBean;
	import com.follett.fsc.core.k12.business.PreferenceManager;
	import com.follett.fsc.core.k12.business.StudentManager;
	import com.follett.fsc.core.k12.tools.reports.ReportDataGrid;
	import com.follett.fsc.core.k12.tools.reports.ReportJavaSourceNet;
	import com.follett.fsc.core.k12.tools.reports.ReportUtils;
	import com.follett.fsc.core.k12.tools.reports.SecondaryStudentDataSource;
	import com.follett.fsc.core.k12.web.UserDataContainer;
	import com.x2dev.sis.model.beans.ConductAction;
	import com.x2dev.sis.model.beans.ConductActionDate;
	import com.x2dev.sis.model.beans.ConductIncident;
	import com.x2dev.sis.model.beans.SisSchool;
	import com.x2dev.sis.model.beans.SisSchoolCalendarDate;
	import com.x2dev.sis.model.beans.SisStudent;
	import com.x2dev.sis.model.beans.StudentAttendance;
	import com.x2dev.sis.model.beans.StudentEnrollment;
	import com.x2dev.sis.model.beans.StudentProgramDetail;
	import com.x2dev.sis.model.beans.StudentProgramParticipation;
	import com.x2dev.sis.model.business.CalendarManager;
	import com.x2dev.sis.model.business.EnrollmentManager;
	import com.x2dev.utils.StringUtils;
	import com.x2dev.utils.X2BaseException;
	import com.x2dev.utils.types.PlainDate;
	 
	import net.sf.jasperreports.engine.JRDataSource;
	
	public class RICHMOND_ATTENDANCE_REVIEW_V3 extends SecondaryStudentDataSource
	{
	
		private static final String							STATUS_ACTIVE = "Active";
		
		private static final String							QUERY_BY = "queryBy";
		 	
		private static final String							DISTRICT_PARAM 					= "districtSummary";
		private static final String							GRADE_PK = "PK"; 
		
		/**
		 *	Start Date Parameter. 
		 */	
		private static final String							START_DATE						= "StartDate"; 
	
		/**
		 *	End Date Parameter.
		 */	
		private static final String							END_DATE						= "EndDate"; 
		/**
		 *	Attendance Reason Code for Out of School Suspension.
		 */
		public  static final String 						OS_CODE 						= "OS";
	
		/**
		 *	Report Field : School
		 */	
		private static final String							FIELD_SCHOOL					= "School";
	
		/**
		 *	Report Field : All Students ADA number
		 */	
		private static final String							FIELD_ALL_ADA_NUM					= "AllAdaNum";
	
		/**
		 *	Report Field : All Students ADA Percent
		 */	
		private static final String							FIELD_ALL_ADA_PER					= "ALLADAPER";
	 
	
		/**
		 *	Report Field : All Students % with 5+ Tardy
		 */	
		private static final String							FIELD_ALL_TAR_PER					= "AllTarPer";
	
		/**
		 *	Report Field : All Students # with 5+ Absences
		 */	
		private static final String							FIELD_ALL_ABS_NUM					= "AllAbsNum";
	
		/**
		 *	Report Field : All Students % with 5+ Absences
		 */	
		private static final String							FIELD_ALL_ABS_PER					= "AllAbsPer";
	 
	
		/**
		 *	Report Field : All Students Total OS Absences
		 */	
		private static final String							FIELD_ALL_OS					= "AllOs";
	 
	
		/**
		 *	Report Field : Disability ADA number
		 */	
		private static final String							FIELD_DIS_ADA_NUM					= "DisAdaNum";
	
		/**
		 *	Report Field : Disability ADA Percent
		 */	
		private static final String							FIELD_DIS_ADA_PER					= "DisAdaPer";
	 
	
		/**
		 *	Report Field : Disability % with 5+ Tardy
		 */	
		private static final String							FIELD_DIS_TAR_PER					= "DisTarPer";
	
		/**
		 *	Report Field : Disability # with 5+ Absences
		 */	
		private static final String							FIELD_DIS_ABS_NUM					= "DisAbsNum";
	
		/**
		 *	Report Field : Disability % with 5+ Absences
		 */	
		private static final String							FIELD_DIS_ABS_PER					= "DisAbsPer";
	 
	
		/**
		 *	Report Field : Disability,  Total OS Absences
		 */		
		private static final String							FIELD_DIS_OS					= "DisOs"; 
	
		/**
		 *	Report Field : ELL ADA number
		 */	
		private static final String							FIELD_ELL_ADA_NUM					= "EllAdaNum";
	
		/**
		 *	Report Field : ELL ADA Percent
		 */	
		private static final String							FIELD_ELL_ADA_PER					= "EllAdaPer";
	 
	
		/**
		 *	Report Field : ELL % with 5+ Tardy
		 */	
		private static final String							FIELD_ELL_TAR_PER					= "EllTarPer";
	
		/**
		 *	Report Field : ELL # with 5+ Absences
		 */	
		private static final String							FIELD_ELL_ABS_NUM					= "EllAbsNum";
	
		/**
		 *	Report Field : ELL % with 5+ Absences
		 */	
		private static final String							FIELD_ELL_ABS_PER					= "EllAbsPer"; 
	
		/**
		 *	Report Field : ELL,  Total OS Absences
		 */	
		private static final String							FIELD_ELL_OS					= "EllOs";
	 
	
		/**
		 *	Report Field : Homeless ADA number
		 */	
		private static final String							FIELD_HOM_ADA_NUM					= "HomAdaNum";
	
		/**
		 *	Report Field : Homeless ADA Percent
		 */	
		private static final String							FIELD_HOM_ADA_PER					= "HomAdaPer";
	 
	
		/**
		 *	Report Field : Homeless % with 5+ Tardy
		 */	
		private static final String							FIELD_HOM_TAR_PER					= "HomTarPer";
	
		/**
		 *	Report Field : Homeless # with 5+ Absences
		 */	
		private static final String							FIELD_HOM_ABS_NUM					= "HomAbsNum";
	
		/**
		 *	Report Field : Homeless % with 5+ Absences
		 */	
		private static final String							FIELD_HOM_ABS_PER					= "HomAbsPer";
	 
	
		/**
		 *	Report Field : Homeless, Total Excused Absences
		 */	
		private static final String							FIELD_HOM_EX					= "HomEx";
	
		/**
		 *	Report Field : Homeless,  Total OS Absences
		 */	
		private static final String							FIELD_HOM_OS					= "HomOs";
	
		/**
		 *	Report Field : Homeless,  Total IS Absences
		 */	
		private static final String							FIELD_HOM_IS					= "HomIs";
		
	
		/**
		 *	Report Field : All, Total Membership
		 */	
		private static final String							FIELD_ALL_MEM					= "AllMem";
	
		/**
		 *	Report Field : Disability,  Total Membership
		 */	
		private static final String							FIELD_DIS_MEM					= "DisMem";
	
		/**
		 *	Report Field : Ell,  Total Membership
		 */	
		private static final String							FIELD_ELL_MEM					= "EllMem";
	
		/**
		 *	Report Field : Homeless,  Total Membership
		 */	
		private static final String							FIELD_HOM_MEM					= "HomMem";
	
		/**
		 *	Report Field : All, ADA percentage excluding OSS
		 */	
		private static final String							FIELD_ALL_ADA_PER_OSS					= "AllAdaPerOss";
	
		/**
		 *	Report Field : Disability,  ADA percentage excluding OSS
		 */	
		private static final String							FIELD_DIS_ADA_PER_OSS					= "DisAdaPerOss";
	
		/**
		 *	Report Field : Ell,  ADA percentage excluding OSS
		 */	
		private static final String							FIELD_ELL_ADA_PER_OSS					= "EllAdaPerOss";
	
		/**
		 *	Report Field : Homeless, ADA percentage excluding OSS
		 */	
		private static final String							FIELD_HOM_ADA_PER_OSS					= "HomAdaPerOss";
		
		
		/**
		 *	Report Field : All, Percent of Student missing 10% of School Year
		 */	
		private static final String							FIELD_ALL_10					= "All10";
		
		/**
		 *	Report Field : All, Number of Student missing 10% of School Year
		 */	
		private static final String							FIELD_ALL_10_NUM					= "All10Num";
	
		/**
		 *	Report Field : Disability,  Percent of Student missing 10% of School Year
		 */	
		private static final String							FIELD_DIS_10					= "Dis10";
	
		/**
		 *	Report Field : Ell,  Percent of Student missing 10% of School Year
		 */	
		private static final String							FIELD_ELL_10					= "Ell10";
	
		/**
		 *	Report Field : Homeless, Percent of Student missing 10% of School Year
		 */	
		private static final String							FIELD_HOM_10					= "Hom10";
		
		
		/**
		 *	Report Field : All, Percent of Student missing 10% of School Year exclude OSS
		 */	
		private static final String							FIELD_ALL_10_OSS					= "All10Oss";
		
		/**
		 *	Report Field : All, Number of Student missing 10% of School Year exclude OSS
		 */	
		private static final String							FIELD_ALL_10_OSS_NUM				= "All10OssNum";
	
		/**
		 *	Report Field : Disability,  Percent of Student missing 10% of School Year exclude OSS
		 */	
		private static final String							FIELD_DIS_10_OSS					= "Dis10Oss";
	
		/**
		 *	Report Field : Ell,  Percent of Student missing 10% of School Year exclude OSS
		 */	
		private static final String							FIELD_ELL_10_OSS					= "Ell10Oss";
	
		/**
		 *	Report Field : Homeless, Percent of Student missing 10% of School Year exclude OSS
		 */	
		private static final String							FIELD_HOM_10_OSS					= "Hom10Oss";
		
		private static final String							CONSTANT_HOMELESS					= "Homeless"; 
		
		private static final String							CONSTANT_ESL					= "ESL"; 
		/**
		 *	Enrollment type Admission.
		 */	
		private static final String							CON_ADMIT						= "E"; 
		
		/**
		 *	Enrollment Type Withdrawal.
		 */	
		private static final String							CON_WITHDRAW					= "W"; 
		private static final String							RICHMOND_ALTERNATIVE 			= "sklX2000000404";
		
		 
	    public static final int INITIAL_STUDENT_CAPACITY = 3000;
	    
	    // Enrollment report status constants
	    private static final int REPORT_STATUS_ADDED = 100;
	    private static final int REPORT_STATUS_ADDED_DROPPED = 200;
	    private static final int REPORT_STATUS_DROPPED = 300;
	    private static final int REPORT_STATUS_DROPPED_ADDED = 400;
	    private static final int REPORT_STATUS_WITHDREW_PRIOR = 500;
		
	 
		private PlainDate			m_YearStartDate; 
		private Map <String,  SisStudent > m_studentMap;
		private SisSchool m_currentSchool;
		private SubQuery m_schoolSub; 
		private Collection<SisSchool> m_schools; 
		private boolean m_districtSummary; 
		private Map <String, StudentProgramParticipation > m_Homeless;
		private Map <String, StudentProgramParticipation > m_ESL;
		private Collection<SisSchool> m_AmSchools;
		private Selection m_selection; 
		private Map<String, Collection<SchoolCalendarDate>> m_calendarDays; 
		private Map<String,HashMap<String, SchoolCalendar>> m_schoolCalendars  ;
		private Map<String, Collection<PairedEnrollment >> m_schoolMemberships; 
		private String					m_activeCode;
		
		private final String[] EXCLUDE_CODES_EN={ "E099","E100","E110","E119","R115","R403","R099","FSCW","NoSho","W016","W115","W118","W119","W201","W212","W214",
				   "W217","W218","W219","W221","W222","W304","W305","W306","W307","W308","W309",
				   "W310","W312","W313","W314","W321","W402","W411","W503","W650","W730","W731",
				   "W732","W870","W880","W960","W961","W970"};
	
	private final String[] EXCLUDE_CODES_WD={  "E099","E100","E104","E105","E106","E107","E108","E109","E110","E111","E113","E119",
					"E120","E121","E203","R111","R115","R201","R212","R214","R216","R217","R218","R219",
					"R298","R302","R312","R402","R403","R415","R416","R417","R418","W119","W650","W730","W731","W732"};
	 
	private final String[] AMELIA_SCHOOLS = {"307","471","S313","313"};
	private final String[] AMELIA_SCHOOLS_SUB = { "471","S313","313"};
	
	private final String[] RAS={  "OIC", "DOO", "101", "Aspire", "Spartan", "PLC"};
	 
	private Map <String, SisStudent> m_students_enr;
	private Map <String, Collection <StudentEnrollment>> m_enrollments;
	private Map <String, BigDecimal[ ]>  m_mobilityRates;
	   
	private Map                 	m_attendanceData = null;
	private Calendar            	m_calendar = null;
	private Map                 	m_calendarData = null;
	private CalendarManager     	m_calendarManager = null;
	private Map                 	m_enrollmentData = null;
	private EnrollmentManager   	m_enrollmentManager = null;
	private PlainDate           	m_startDate = null;
	private PlainDate           	m_endDate = null;
	private boolean					m_excludeWithdrawnStudents;
	private Set                 	m_priorEnrollments = null; 
	private Collection<String>  	m_excludedGrades;
	private int						m_ageLimit = 18;						// Exclude students of this age and older
	private int						m_truancyThreshold = 10;				// Limit for number of unexcused absences
	private String          		m_schoolOids;
	private Collection<String>  	m_schoolOidsList = null; 
	
	// Variables for student, kept until deciding if they are truant
	private int 					m_daysEnrolled; 
	private double					m_daysUnexcused; 
	private Map <String, Set<String>> m_students;
	private int 					m_daysInSession;
	private StudentEnrollment 		m_enrollment;
	private int 					m_reportStatus;
	private DistrictSchoolYearContext m_currentYear; 
	private Map<String,HashMap<PlainDate,StudentAttendance> > m_absences; 
	// Counts for Truancy Statistics
	private int					 	m_schoolDaysCount;
	private double 					m_notPresent;
	private double 					m_notPresentExcused;
	private double					m_truantStdsCount; 
	private double					m_ttlDaysInSession; 
	private double					m_sumDaysEnrolled;
	private double					m_avgDailyMembership;
	private double					m_truancyRate;
	
	private double					m_truantStdsCountD;
	private double					m_truantStdsCountE;
	private double					m_truantStdsCountH;
	
	private double 					m_truancyRateD;
	private double 					m_truancyRateE;
	private double 					m_truancyRateH;
	
	private double					m_sumDaysEnrolledD;
	private double					m_sumDaysEnrolledE;
	private double					m_sumDaysEnrolledH;
	
	private double					m_avgDailyMembershipE;
	private double					m_avgDailyMembershipD;
	private double					m_avgDailyMembershipH;
	private Map	<String, BigDecimal > m_truancy;
	private Map	<String, BigDecimal > m_truancyD;
	private Map	<String, BigDecimal > m_truancyE;
	private Map	<String, BigDecimal > m_truancyH;
	 
	
	 
	
	// end attributes
	
	/**
	 * @see com.follett.fsc.core.k12.tools.reports.ReportJavaSourceDori#gatherData()
	 */
	@Override
	// start methods
	protected JRDataSource gatherData()
	{
		m_truancy = new HashMap  <String, BigDecimal >();
		m_truancyD = new HashMap  <String, BigDecimal >();
		m_truancyE = new HashMap  <String, BigDecimal >();
		m_truancyH = new HashMap  <String, BigDecimal >();
	 
	
	m_startDate = (PlainDate) getParameter(START_DATE);
	m_endDate = (PlainDate) getParameter(END_DATE );
	
		
		m_schoolSub = getSchoolSubQuery();
		if (m_endDate.compareTo(m_startDate) < 0)
		{
			m_endDate = m_startDate;
		}
		
	 
		
		getSchoolYear(); 
		LoadHomelessStatus();
		LoadESLStatus();
		m_YearStartDate = m_currentYear.getStartDate();
		if (m_endDate.compareTo(m_currentYear.getEndDate())>0 ) m_endDate = m_currentYear.getEndDate();
		
		addParameter(START_DATE,m_startDate );
		addParameter(END_DATE,m_endDate );
		
		ReportDataGrid grid = new ReportDataGrid();  
		LoadStudents();
		loadCalendarMaps();
		LoadAttendance(); 
		LoadHomelessStatus();
	    getMobilityRates();
		getTruancy(); 
		getMobilityRates();
		
		Map<String,Collection<PairedEnrollment>> Enrollments =populateEnrollments();
		 
			processAttendance(grid, Enrollments);
	 
		grid.sort(FIELD_SCHOOL, false);
		grid.beforeTop();
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
	 
	
	public void LoadStudents()
	{
		Criteria criteria = new Criteria();
		/*if (isSchoolContext())
		{
			criteria.addEqualTo(SisStudent.COL_SCHOOL_OID, getSchool().getOid());
		}  */
		 m_enrollmentManager = new EnrollmentManager(getBroker(), getPrivilegeSet(), getOrganization());
		 m_students = new HashMap<String, Set<String>>();
		Criteria criteriaA = new Criteria();
		Criteria criteriaB = new Criteria();
		Criteria criteriaC = new Criteria();
		Criteria criteriaD = new Criteria();
		Criteria comboCriteria = new Criteria();
		Criteria comboCriteria2 = new Criteria();
	
		// Active student
		criteriaA.addEqualTo(SisStudent.COL_ENROLLMENT_STATUS, STATUS_ACTIVE);
		criteriaA.addIn(SisStudent.COL_SCHOOL_OID, m_schoolSub);
	
		// OR anyone who has enrollment record this year.
		SubQuery  enrs = getCurrentYearEnrollments() ;
		criteriaB.addIn(SisStudent.COL_OID, enrs );
	
		comboCriteria.addOrCriteria(criteriaA);
		comboCriteria.addOrCriteria(criteriaB);
	
		criteria.addAndCriteria(comboCriteria);  //comboCriteria
		
		criteriaC.addEqualTo(  SisStudent.COL_FIELD_A032 , "0");
		criteriaD.addEqualTo(  SisStudent.COL_FIELD_A032 , null);
		comboCriteria2.addOrCriteria(criteriaC);
		comboCriteria2.addOrCriteria(criteriaD);
		
		criteria.addAndCriteria(comboCriteria2);  //comboCriteria
		criteria.addNotEqualTo(SisStudent.COL_GRADE_LEVEL, GRADE_PK);
	
		QueryByCriteria query =  new QueryByCriteria(SisStudent.class, criteria);
		query.setDistinct(true);
		for (SisSchool school :m_schools)
		{	
		m_students.put(school.getOid(),   m_enrollmentManager.getMembershipAsOf(m_endDate, school));
		}
		
	//	m_students   = getBroker().getGroupedCollectionByQuery(query, SisStudent.COL_SCHOOL_OID, 100); 
		m_studentMap = getBroker().getMapByQuery(query, SisStudent.COL_OID, 1000);
	
		m_selection  = (Selection) X2BaseBean.newInstance(Selection.class, getBroker().getPersistenceKey());
	
		// add students to selection object table
		
		Collection<SisStudent>  STU_COL = m_studentMap.values();
	
		for (SisStudent student : STU_COL)
		{
			SelectionObject selectedObject = (SelectionObject) X2BaseBean.newInstance(SelectionObject.class, getBroker().getPersistenceKey());
			selectedObject.setObjectOid(student.getOid());   
			m_selection.addToSelectionObjects(selectedObject);  
		} 
		m_selection.setTimestamp(System.currentTimeMillis());
		getBroker().saveBean(m_selection);
	}
	
	 
	
	 
	
	public void LoadAttendance()
	{
		Criteria criteria = new Criteria();
		criteria.addExists(getSubQueryFromSelection(StudentAttendance.COL_STUDENT_OID )); 
		QueryByCriteria query =  new QueryByCriteria(StudentAttendance.class, criteria);
		query.setDistinct(true);
		m_absences = getBroker().getNestedMapByQuery(query, StudentAttendance.COL_STUDENT_OID, StudentAttendance.COL_DATE, 1000, 200);
	
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
	
	
	private SubQuery getCurrentYearEnrollments()
	{
		Criteria criteria = new Criteria();
		criteria.addGreaterOrEqualThan(StudentEnrollment.COL_ENROLLMENT_DATE, m_YearStartDate);
		criteria.addIn(StudentEnrollment.COL_SCHOOL_OID , m_schoolSub);
	
		SubQuery query = new SubQuery(StudentEnrollment.class, StudentEnrollment.COL_STUDENT_OID, criteria);   
		query.setDistinct(true);
		return query;
	}
	
	private void processAttendance(final ReportDataGrid grid, Map<String,Collection<PairedEnrollment>> Enrollments )
	{
	
		Collection<String> SchoolOids = m_students.keySet();
	
		for (String Oid: SchoolOids)
		{
			Collection<String> students = m_students.get(Oid);
	
			for (String student : students)
			{
				if (Enrollments.containsKey(student ))
				{
				Collection<PairedEnrollment> Enr = Enrollments.get(student );
	
				for (PairedEnrollment PE: Enr)
				{
			 		ProcessMembership(PE,  student );
				}
				}
			}
		}
	
		 
	
		for(SisSchool school: m_schools)
		{
			String Oid = school.getOid();
			
		if  (m_schoolCalendars.containsKey(Oid) && m_schoolMemberships.containsKey(Oid) && m_students.containsKey(Oid) )
		{
			
			String key = m_schoolCalendars.get(Oid).values().iterator().next().getOid();
		if (key != null && m_calendarDays.containsKey (key ))	
		{
			int days =   m_calendarDays.get(key).size();
			/// Variables for All Students Row
			BigDecimal 		AAN=BigDecimal.ZERO;
			int 		AANAbs =0;
			int 		AANAbo =0;
			int 		AANMem =0;
			BigDecimal 	AAP = BigDecimal.ZERO; 
			int			ABN = 0;
			BigDecimal 	ABP = BigDecimal.ZERO; 
			int			AOS = 0; 
			int 		A10N =0;
			int			A10NO = 0;
			BigDecimal  AAPO= BigDecimal.ZERO;
			BigDecimal  A10 = BigDecimal.ZERO;
			BigDecimal  A10O= BigDecimal.ZERO;
			/// Variables for Disabled Students Row
			BigDecimal 		DAN=BigDecimal.ZERO;
			int 		DANAbs=0;
			int 		DANAbo=0;
			int 		DANMem=0;
			BigDecimal 	DAP = BigDecimal.ZERO; 
			int			DBN = 0;
			BigDecimal 	DBP = BigDecimal.ZERO; 
			int			DOS = 0; 
			int 		D10N =0;
			int			D10NO = 0;
			BigDecimal  DAPO= BigDecimal.ZERO;
			BigDecimal  D10 = BigDecimal.ZERO;
			BigDecimal  D10O= BigDecimal.ZERO;
			/// Variables for ELL Students Row
			BigDecimal 		EAN=BigDecimal.ZERO;
			int 		EANAbs=0;
			int 		EANAbo=0;
			int 		EANMem=0;
			BigDecimal 	EAP = BigDecimal.ZERO; 
			int			EBN = 0;
			BigDecimal 	EBP = BigDecimal.ZERO; 
			int			EOS = 0; 
			int 		E10N =0;
			int			E10NO = 0;
			BigDecimal  EAPO= BigDecimal.ZERO;
			BigDecimal  E10 = BigDecimal.ZERO;
			BigDecimal  E10O= BigDecimal.ZERO;
			/// Variables for Homeless Students Row
			BigDecimal 		HAN=BigDecimal.ZERO;
			int 		HANAbs=0;
			int 		HANAbo=0;
			int 		HANMem=0;
			BigDecimal 	HAP = BigDecimal.ZERO; 
			int			HBN = 0;
			BigDecimal 	HBP = BigDecimal.ZERO; 
			int			HOS = 0; 
			int 		H10N =0;
			int			H10NO = 0;
			BigDecimal  HAPO= BigDecimal.ZERO;
			BigDecimal  H10 = BigDecimal.ZERO;
			BigDecimal  H10O= BigDecimal.ZERO;
			// Process ADA
			Collection<PairedEnrollment> Enrs = m_schoolMemberships.get(Oid);
			for (PairedEnrollment Enr : Enrs)
			{
				SisStudent student = m_studentMap.get(Enr.studentOid);
				AANAbs= AANAbs + Enr.Absences;
				AANAbo= AANAbo + Enr.Absences - Enr.OSS;
				AANMem=  AANMem +Enr.Membership_days; 
				
				if (student.getSpedStatusCode()  != null &&  student.getSpedStatusCode().equals(STATUS_ACTIVE) )  
				{
					DANAbs= DANAbs+ Enr.Absences;
					DANAbo= DANAbo+ Enr.Absences - Enr.OSS;
					DANMem= DANMem + Enr.Membership_days;
				}
	
				if (m_ESL.containsKey(student.getOid()) )
				{
					EANAbs= EANAbs+ Enr.Absences;
					EANAbo= EANAbo+ Enr.Absences - Enr.OSS;
					EANMem= EANMem + Enr.Membership_days;
				}
	
				//if (student.getFieldA097() != null && (student.getFieldA097().equals("Y") || student.getFieldA097().equals("1")) )
				if (m_Homeless.containsKey(student.getOid()))
				{
					HANAbs= HANAbs+ Enr.Absences;
					HANAbo= HANAbo+ Enr.Absences - Enr.OSS;
					HANMem= HANMem + Enr.Membership_days;
				}
	
			}
			if (AANMem >0)
			{
			// All Students ADA
			BigDecimal NTemp  = new BigDecimal(AANMem).subtract( new BigDecimal( AANAbs));
			AAP = NTemp.divide(new BigDecimal(AANMem),4,3); 
			AAPO= new BigDecimal(AANMem).subtract( new BigDecimal( AANAbo)).divide(new BigDecimal(AANMem),4,3);
			AAN = NTemp.divide(new BigDecimal(days), 1, RoundingMode.UP); 
			grid.append();
			grid.set(FIELD_SCHOOL,  school.getName()); 
			 grid.set(FIELD_SCHOOL,  school.getName()); 
			grid.set(FIELD_ALL_ADA_NUM, AAN);  
			grid.set(FIELD_ALL_ADA_PER, AAP);
			grid.set(FIELD_ALL_ADA_PER_OSS, AAPO);
			grid.set(FIELD_ALL_MEM, new BigDecimal(AANMem));
	
			// Disability Students ADA
			if (DANMem >0)
			{
				NTemp  = new BigDecimal(DANMem).subtract( new BigDecimal( DANAbs));
				DAP = NTemp.divide(new BigDecimal(DANMem),4,3); 
				DAPO= new BigDecimal(DANMem).subtract( new BigDecimal( DANAbo)).divide(new BigDecimal(DANMem),4,3);
				DAN = NTemp.divide(new BigDecimal(days), 1, RoundingMode.UP); 
	
				grid.set(FIELD_DIS_ADA_PER, DAP);
				grid.set(FIELD_DIS_ADA_PER_OSS, DAPO);
				grid.set(FIELD_DIS_MEM, new BigDecimal(DANMem));
			}
			else
			{ 
				grid.set(FIELD_DIS_ADA_NUM, null);
				grid.set(FIELD_DIS_ADA_PER, null);
				grid.set(FIELD_DIS_ADA_PER_OSS, null);
				grid.set(FIELD_DIS_MEM, new BigDecimal(DANMem));
	
			}
	
	
			// ELL Students ADA
			if (EANMem >0)
			{
				NTemp  = new BigDecimal(EANMem).subtract( new BigDecimal( EANAbs));
				EAP = NTemp.divide(new BigDecimal(EANMem),4,3); 
				EAPO= new BigDecimal(EANMem).subtract( new BigDecimal( EANAbo)).divide(new BigDecimal(EANMem),4,3);
				EAN = NTemp.divide(new BigDecimal(days), 1, RoundingMode.UP);	 
				grid.set(FIELD_ELL_ADA_PER, EAP);
				grid.set(FIELD_ELL_ADA_PER_OSS, EAPO);
				grid.set(FIELD_ELL_MEM, new BigDecimal(EANMem));
			}
			else
			{ 
				grid.set(FIELD_ELL_ADA_PER, null);
				grid.set(FIELD_ELL_ADA_PER_OSS, null);
				grid.set(FIELD_ELL_MEM, new BigDecimal(EANMem));
			}
	
			// Homeless Students ADA
			if (HANMem >0)
			{
				NTemp  = new BigDecimal(HANMem).subtract( new BigDecimal( HANAbs));
				HAP = NTemp.divide(new BigDecimal(HANMem),4,3); 
				HAPO= new BigDecimal(HANMem).subtract( new BigDecimal( HANAbo)).divide(new BigDecimal(HANMem),4,3);
				HAN = NTemp.divide(new BigDecimal(days), 1, RoundingMode.UP);  
				grid.set(FIELD_HOM_ADA_PER, HAP);
				grid.set(FIELD_HOM_ADA_PER_OSS, HAPO);
				grid.set(FIELD_HOM_MEM, new BigDecimal(HANMem));
			}
			else
			{ 
				grid.set(FIELD_HOM_ADA_PER, null);
				grid.set(FIELD_HOM_ADA_PER_OSS, null);	
				grid.set(FIELD_HOM_MEM, new BigDecimal(HANMem));
			}
	
			/// Process non-ADA stats
			Collection<String> sOids = m_students.get(Oid);
			int studentCount = 0;
			int disCount = 0;
			int EllCount = 0;
			int HomeCount = 0;
	
			for (String sOid : sOids)
			{
			if ( Enrollments != null && Enrollments.containsKey(sOid ))
			{
				Collection<PairedEnrollment> Enr = Enrollments.get(sOid );
				int AllAbsNum = 0;
				int AllOSS = 0;
				int Memb = 0; 
				int DisAbsNum = 0; 
				int DisOSS = 0;
				int EllAbsNum =0; 
				int ELLOSS = 0;
				int HomAbsNum = 0; 
				int HomOSS = 0;
				SisStudent student = m_studentMap.get(sOid);	
				if (Enr !=null )
				{
					studentCount = studentCount +1;	
					if (student.getSpedStatusCode()  != null &&  student.getSpedStatusCode().equals(STATUS_ACTIVE) ) disCount = disCount +1;
					if (m_ESL.containsKey(student.getOid())) EllCount = EllCount+1;
					if (m_Homeless.containsKey(student.getOid())) HomeCount = HomeCount+1;
					
					for (PairedEnrollment PE: Enr)
					{
						AllAbsNum = AllAbsNum+ PE.Absences; 
						AOS = AOS + PE.OSS;
						AllOSS = AllOSS + PE.OSS; 
						Memb = Memb + PE.Membership_days;
	
						 if (student.getSpedStatusCode()  != null &&  student.getSpedStatusCode().equals(STATUS_ACTIVE) )  
						{
							 
							DisAbsNum= DisAbsNum+ PE.Absences; 
							DOS = DOS + PE.OSS;
							DisOSS = DisOSS + PE.OSS; 
						}
	
						if (m_ESL.containsKey(student.getOid()))
						{ 
							EllAbsNum= EllAbsNum+ PE.Absences; 
							EOS = EOS + PE.OSS;
							ELLOSS = ELLOSS + PE.OSS; 
						}
	
						//if (student.getFieldA097() != null && (student.getFieldA097().equals("Y") || student.getFieldA097().equals("1")) )
						if (m_Homeless.containsKey(student.getOid()))
						{ 
							HomAbsNum= HomAbsNum+ PE.Absences;  
							HOS = HOS + PE.OSS;
							HomOSS = HomOSS + PE.OSS; 
						} 
					}
	
					if (AllAbsNum >=m_truancyThreshold &&  Years.yearsBetween(new LocalDate (student.getPerson().getDob()), new LocalDate(m_endDate)).getYears() <18) {ABN = ABN+1; } 
					if (DisAbsNum >=m_truancyThreshold &&  Years.yearsBetween(new LocalDate (student.getPerson().getDob()), new LocalDate(m_endDate)).getYears() <18) {DBN = DBN+1; } 
					if (EllAbsNum >=m_truancyThreshold &&  Years.yearsBetween(new LocalDate (student.getPerson().getDob()), new LocalDate(m_endDate)).getYears() <18) {EBN = EBN+1; } 
					if (HomAbsNum >=m_truancyThreshold &&  Years.yearsBetween(new LocalDate (student.getPerson().getDob()), new LocalDate(m_endDate)).getYears() <18) {HBN = HBN+1; } 
					
					// Calculate if student missed 10% of membership days
					if (Memb >0)
					{
					if(new BigDecimal(AllAbsNum).divide(new BigDecimal(Memb),4,3).compareTo(new BigDecimal(.095)) >=0 && !student.getGradeLevel().equals("PK"))
					{  	A10N =A10N+1;}
					
					if(new BigDecimal(AllAbsNum).subtract(new BigDecimal(AllOSS)).divide(new BigDecimal(Memb),4,3).compareTo(new BigDecimal(.095)) >=0 && !student.getGradeLevel().equals("PK"))
					{  	A10NO =A10NO+1;}
					
					if(new BigDecimal(DisAbsNum).divide(new BigDecimal(Memb),4,3).compareTo(new BigDecimal(.095)) >=0 && !student.getGradeLevel().equals("PK"))
					{  	D10N =D10N+1;}
					if(new BigDecimal(DisAbsNum).subtract(new BigDecimal(DisOSS)).divide(new BigDecimal(Memb),4,3).compareTo(new BigDecimal(.095)) >=0 && !student.getGradeLevel().equals("PK"))
					{  	D10NO =D10NO+1;}
					
					if(new BigDecimal(EllAbsNum).divide(new BigDecimal(Memb),4,3).compareTo(new BigDecimal(.095)) >=0 && !student.getGradeLevel().equals("PK"))
					{  	E10N =E10N+1;}
					if(new BigDecimal(EllAbsNum).subtract(new BigDecimal(ELLOSS)).divide(new BigDecimal(Memb),4,3).compareTo(new BigDecimal(.095)) >=0 && !student.getGradeLevel().equals("PK"))
					{  	E10NO =E10NO+1;}
					
					if(new BigDecimal(HomAbsNum).divide(new BigDecimal(Memb),4,3).compareTo(new BigDecimal(.095)) >=0 && !student.getGradeLevel().equals("PK"))
					{  	H10N =H10N+1;}
					if(new BigDecimal(HomAbsNum).subtract(new BigDecimal(HomOSS)).divide(new BigDecimal(Memb),4,3).compareTo(new BigDecimal(.095)) >=0 && !student.getGradeLevel().equals("PK"))
					{  	H10NO =H10NO+1;}
					}
				}
			}
			} // For each students
			
			
		 
			
			if (studentCount >0)
			{ 
			A10 = new BigDecimal(A10N).divide(new BigDecimal(studentCount),4,3);
			A10O = new BigDecimal(A10NO).divide(new BigDecimal(studentCount),4,3);
			}
			if (m_truancy.containsKey(school.getOid())) ABP= m_truancy.get(school.getOid());
			else ABP = BigDecimal.ZERO;
				
			grid.set(FIELD_ALL_ADA_NUM, studentCount);
			grid.set(FIELD_ALL_ABS_NUM, ABN);
			grid.set(FIELD_ALL_ABS_PER, ABP); 
			grid.set(FIELD_ALL_OS, AOS); 
			grid.set(FIELD_ALL_10, A10);
			grid.set(FIELD_ALL_10_NUM, A10N);
			grid.set(FIELD_ALL_10_OSS, A10O);
			grid.set(FIELD_ALL_10_OSS_NUM, A10NO);
	
			if (m_mobilityRates.containsKey(school.getOid()))
			{
			BigDecimal[] rates = m_mobilityRates.get(school.getOid());	
			grid.set(FIELD_ALL_TAR_PER, rates[0]);
			grid.set(FIELD_DIS_TAR_PER, rates[1]);	
			grid.set(FIELD_ELL_TAR_PER, rates[2]);	
			grid.set(FIELD_HOM_TAR_PER, rates[3]);	
			} 
			
			grid.set(FIELD_DIS_OS, 		DOS);  
			grid.set(FIELD_ELL_OS, 		EOS); 
			grid.set(FIELD_HOM_OS, 		HOS); 
	
			if (disCount >0){ 
				if (m_truancyD.containsKey(school.getOid())) DBP= m_truancyD.get(school.getOid());
				else DBP = BigDecimal.ZERO;
				
				D10 = new BigDecimal(D10N).divide(new BigDecimal(disCount),4,3);
				D10O = new BigDecimal(D10NO).divide(new BigDecimal(disCount),4,3);
				grid.set(FIELD_DIS_ADA_NUM, disCount);
				grid.set(FIELD_DIS_ABS_NUM, DBN);
				grid.set(FIELD_DIS_ABS_PER, DBP); 
				grid.set(FIELD_DIS_10, D10);
				grid.set(FIELD_DIS_10_OSS, D10O);
	
			}
			else
			{
				grid.set(FIELD_DIS_ADA_NUM, 0);
				grid.set(FIELD_DIS_ABS_NUM, null);
				grid.set(FIELD_DIS_ABS_PER, null); 
				grid.set(FIELD_DIS_10, null);
				grid.set(FIELD_DIS_10_OSS, null);
	
			}
	
			if (EllCount >0){ 
				if (m_truancyE.containsKey(school.getOid())) EBP= m_truancyE.get(school.getOid());
				else EBP = BigDecimal.ZERO;
				E10 = new BigDecimal(E10N).divide(new BigDecimal(EllCount),4,3);
				E10O = new BigDecimal(E10NO).divide(new BigDecimal(EllCount),4,3);
				grid.set(FIELD_ELL_ADA_NUM, EllCount);
				grid.set(FIELD_ELL_ABS_NUM, EBN);
				grid.set(FIELD_ELL_ABS_PER, EBP); 
				grid.set(FIELD_ELL_10, E10);
				grid.set(FIELD_ELL_10_OSS, E10O);
			}
			else
			{
				grid.set(FIELD_ELL_ADA_NUM, 0);
				grid.set(FIELD_ELL_ABS_NUM, null);
				grid.set(FIELD_ELL_ABS_PER, null); 
				grid.set(FIELD_ELL_10, null);
				grid.set(FIELD_ELL_10_OSS, null);
	
			}
	
			if (HomeCount >0){  
				if (m_truancyH.containsKey(school.getOid())) HBP= m_truancyH.get(school.getOid());
				else HBP = BigDecimal.ZERO;
				H10 = new BigDecimal(H10N).divide(new BigDecimal(HomeCount),4,3);
				H10O = new BigDecimal(H10NO).divide(new BigDecimal(HomeCount),4,3);
				grid.set(FIELD_HOM_ADA_NUM, HomeCount);
				grid.set(FIELD_HOM_ABS_NUM, HBN);
				grid.set(FIELD_HOM_ABS_PER, HBP); 
				grid.set(FIELD_HOM_10, H10);
				grid.set(FIELD_HOM_10_OSS, H10O);
			}
			else
			{
				grid.set(FIELD_HOM_ADA_NUM, 0);
				grid.set(FIELD_HOM_ABS_NUM, null);
				grid.set(FIELD_HOM_ABS_PER, null); 
				grid.set(FIELD_HOM_10, null);
				grid.set(FIELD_HOM_10_OSS, null);
	
			}
		 } 
		if (Oid.equals( RICHMOND_ALTERNATIVE)) processRAS(  grid,   Enrollments );
		}
		}
		} // for each school
	
	}
	
	
	private void processRAS(final ReportDataGrid grid, Map<String,Collection<PairedEnrollment>> Enrollments )
	{
	
	 Map<String, Collection <SisStudent>> m_rasStudents = LoadRASStudents();
	Collection<String> RasSchools = m_rasStudents.keySet();
	
	Collection<String> rasSchoolCodes = new ArrayList<String>(Arrays.asList(RAS)); 
	String Oid = RICHMOND_ALTERNATIVE;
	
		for(String school: rasSchoolCodes)
		{
			 Collection<SisStudent> students = new HashSet<SisStudent>();
			 
			 for (String hrm : RasSchools){
				 if (hrm.toUpperCase().substring(0, 3).equals(school.toUpperCase().substring(0, 3)) )
				 {
					students.addAll(m_rasStudents.get(hrm)); 
				 }
			 }
	 
			
		if  (m_schoolCalendars.containsKey(Oid) && m_schoolMemberships.containsKey(Oid) && students.size() >0 )
		{
			grid.append();
			grid.set(FIELD_SCHOOL, "Richmond Alternative School - "+ school ); 
			String key = m_schoolCalendars.get(Oid).values().iterator().next().getOid();
		if (key != null && m_calendarDays.containsKey (key ))	
		{
			int days =   m_calendarDays.get(key).size();
			/// Variables for All Students Row
			BigDecimal 		AAN=BigDecimal.ZERO;
			int 		AANAbs =0;
			int 		AANAbo =0;
			int 		AANMem =0;
			BigDecimal 	AAP = BigDecimal.ZERO; 
			int			ABN = 0;
			BigDecimal 	ABP = BigDecimal.ZERO; 
			int			AOS = 0; 
			int 		A10N =0;
			int			A10NO = 0;
			BigDecimal  AAPO= BigDecimal.ZERO;
			BigDecimal  A10 = BigDecimal.ZERO;
			BigDecimal  A10O= BigDecimal.ZERO;
			/// Variables for Disabled Students Row
			BigDecimal 		DAN=BigDecimal.ZERO;
			int 		DANAbs=0;
			int 		DANAbo=0;
			int 		DANMem=0;
			BigDecimal 	DAP = BigDecimal.ZERO; 
			int			DBN = 0;
			BigDecimal 	DBP = BigDecimal.ZERO; 
			int			DOS = 0; 
			int 		D10N =0;
			int			D10NO = 0;
			BigDecimal  DAPO= BigDecimal.ZERO;
			BigDecimal  D10 = BigDecimal.ZERO;
			BigDecimal  D10O= BigDecimal.ZERO;
			/// Variables for ELL Students Row
			BigDecimal 		EAN=BigDecimal.ZERO;
			int 		EANAbs=0;
			int 		EANAbo=0;
			int 		EANMem=0;
			BigDecimal 	EAP = BigDecimal.ZERO; 
			int			EBN = 0;
			BigDecimal 	EBP = BigDecimal.ZERO; 
			int			EOS = 0; 
			int 		E10N =0;
			int			E10NO = 0;
			BigDecimal  EAPO= BigDecimal.ZERO;
			BigDecimal  E10 = BigDecimal.ZERO;
			BigDecimal  E10O= BigDecimal.ZERO;
			/// Variables for Homeless Students Row
			BigDecimal 		HAN=BigDecimal.ZERO;
			int 		HANAbs=0;
			int 		HANAbo=0;
			int 		HANMem=0;
			BigDecimal 	HAP = BigDecimal.ZERO; 
			int			HBN = 0;
			BigDecimal 	HBP = BigDecimal.ZERO; 
			int			HOS = 0; 
			int 		H10N =0;
			int			H10NO = 0;
			BigDecimal  HAPO= BigDecimal.ZERO;
			BigDecimal  H10 = BigDecimal.ZERO;
			BigDecimal  H10O= BigDecimal.ZERO; 
			
			 
			
			int studentCount = 0;
			int disCount = 0;
			int EllCount = 0;
			int HomeCount = 0;
	
			for (SisStudent student : students)
			{
			if (student.getEnrollmentStatus() != null &&student.getEnrollmentStatus().equals(STATUS_ACTIVE) && Enrollments != null && Enrollments.containsKey(student.getOid()))
			{
				Collection<PairedEnrollment> Enr = Enrollments.get(student.getOid());
				int AllAbsNum = 0;
				int AllOSS = 0;
				int Memb = 0; 
				int DisAbsNum = 0; 
				int DisOSS = 0;
				int EllAbsNum =0; 
				int ELLOSS = 0;
				int HomAbsNum = 0; 
				int HomOSS = 0;
	
				if (Enr !=null )
				{
					studentCount = studentCount +1;	
					if (student.getSpedStatusCode()  != null &&  student.getSpedStatusCode().equals(STATUS_ACTIVE) ) disCount = disCount +1;
					if (m_ESL.containsKey(student.getOid())) EllCount = EllCount+1;
					if (m_Homeless.containsKey(student.getOid())) HomeCount = HomeCount+1;
					
					for (PairedEnrollment PE: Enr)
					{
						/// Exclude absences before 5th Birthday
						AllAbsNum = AllAbsNum+ PE.Absences; 
						AOS = AOS + PE.OSS;
						AllOSS = AllOSS + PE.OSS; 
						Memb = Memb + PE.Membership_days;
					 
						if (PE.SchoolOid.equals(RICHMOND_ALTERNATIVE)){
						AANAbs= AANAbs + PE.Absences;
						AANAbo= AANAbo + PE.Absences - PE.OSS;
						AANMem=  AANMem +PE.Membership_days; }
						  
						 if (student.getSpedStatusCode()  != null &&  student.getSpedStatusCode().equals(STATUS_ACTIVE) )  
						{
							 
							DisAbsNum= DisAbsNum+ PE.Absences; 
							DOS = DOS + PE.OSS;
							DisOSS = DisOSS + PE.OSS; 
							if (PE.SchoolOid.equals(RICHMOND_ALTERNATIVE)){
							DANAbs= DANAbs+ PE.Absences;
							DANAbo= DANAbo+ PE.Absences - PE.OSS;
							DANMem= DANMem + PE.Membership_days;}
						}
	
						if (m_ESL.containsKey(student.getOid()))
						{ 
							EllAbsNum= EllAbsNum+ PE.Absences; 
							EOS = EOS + PE.OSS;
							ELLOSS = ELLOSS + PE.OSS; 
							if (PE.SchoolOid.equals(RICHMOND_ALTERNATIVE)){
							EANAbs= EANAbs+ PE.Absences;
							EANAbo= EANAbo+ PE.Absences - PE.OSS;
							EANMem= EANMem + PE.Membership_days;}
						}
	
						//if (student.getFieldA097() != null && (student.getFieldA097().equals("Y") || student.getFieldA097().equals("1")) )
						if (m_Homeless.containsKey(student.getOid()))
						{ 
							HomAbsNum= HomAbsNum+ PE.Absences;  
							HOS = HOS + PE.OSS;
							HomOSS = HomOSS + PE.OSS; 
							if (PE.SchoolOid.equals(RICHMOND_ALTERNATIVE)){
							HANAbs= HANAbs+ PE.Absences;
							HANAbo= HANAbo+ PE.Absences - PE.OSS;
							HANMem= HANMem + PE.Membership_days;}
						} 
					}
	
	 
					// Calculate if student missed 10% of membership days
					if (Memb >0)
					{
					if(new BigDecimal(AllAbsNum).divide(new BigDecimal(Memb),4,3).compareTo(new BigDecimal(.095)) >=0 && !student.getGradeLevel().equals("PK"))
					{  	A10N =A10N+1;}
					
					if(new BigDecimal(AllAbsNum).subtract(new BigDecimal(AllOSS)).divide(new BigDecimal(Memb),4,3).compareTo(new BigDecimal(.095)) >=0 && !student.getGradeLevel().equals("PK"))
					{  	A10NO =A10NO+1;}
					
					if(new BigDecimal(DisAbsNum).divide(new BigDecimal(Memb),4,3).compareTo(new BigDecimal(.095)) >=0 && !student.getGradeLevel().equals("PK"))
					{  	D10N =D10N+1;}
					if(new BigDecimal(DisAbsNum).subtract(new BigDecimal(DisOSS)).divide(new BigDecimal(Memb),4,3).compareTo(new BigDecimal(.095)) >=0 && !student.getGradeLevel().equals("PK"))
					{  	D10NO =D10NO+1;}
					
					if(new BigDecimal(EllAbsNum).divide(new BigDecimal(Memb),4,3).compareTo(new BigDecimal(.095)) >=0 && !student.getGradeLevel().equals("PK"))
					{  	E10N =E10N+1;}
					if(new BigDecimal(EllAbsNum).subtract(new BigDecimal(ELLOSS)).divide(new BigDecimal(Memb),4,3).compareTo(new BigDecimal(.095)) >=0 && !student.getGradeLevel().equals("PK"))
					{  	E10NO =E10NO+1;}
					
					if(new BigDecimal(HomAbsNum).divide(new BigDecimal(Memb),4,3).compareTo(new BigDecimal(.095)) >=0 && !student.getGradeLevel().equals("PK"))
					{  	H10N =H10N+1;}
					if(new BigDecimal(HomAbsNum).subtract(new BigDecimal(HomOSS)).divide(new BigDecimal(Memb),4,3).compareTo(new BigDecimal(.095)) >=0 && !student.getGradeLevel().equals("PK"))
					{  	H10NO =H10NO+1;}
					}
				}
			}
			} // For each students
			
			
			// All Students ADA
						BigDecimal NTemp  = new BigDecimal(AANMem).subtract( new BigDecimal( AANAbs));
						AAP = NTemp.divide(new BigDecimal(AANMem),4,3); 
						AAPO= new BigDecimal(AANMem).subtract( new BigDecimal( AANAbo)).divide(new BigDecimal(AANMem),4,3);
						AAN = NTemp.divide(new BigDecimal(days), 1, RoundingMode.UP); 
						
						grid.set(FIELD_ALL_ADA_NUM, AAN);  
						grid.set(FIELD_ALL_ADA_PER, AAP);
						grid.set(FIELD_ALL_ADA_PER_OSS, AAPO);
						grid.set(FIELD_ALL_MEM, new BigDecimal(AANMem));
	
						// Disability Students ADA
						if (DANMem >0)
						{
							NTemp  = new BigDecimal(DANMem).subtract( new BigDecimal( DANAbs));
							DAP = NTemp.divide(new BigDecimal(DANMem),4,3); 
							DAPO= new BigDecimal(DANMem).subtract( new BigDecimal( DANAbo)).divide(new BigDecimal(DANMem),4,3);
							DAN = NTemp.divide(new BigDecimal(days), 1, RoundingMode.UP); 
	
							grid.set(FIELD_DIS_ADA_PER, DAP);
							grid.set(FIELD_DIS_ADA_PER_OSS, DAPO);
							grid.set(FIELD_DIS_MEM, new BigDecimal(DANMem));
						}
						else
						{ 
							grid.set(FIELD_DIS_ADA_NUM, null);
							grid.set(FIELD_DIS_ADA_PER, null);
							grid.set(FIELD_DIS_ADA_PER_OSS, null);
							grid.set(FIELD_DIS_MEM, new BigDecimal(DANMem));
	
						}
	
	
						// ELL Students ADA
						if (EANMem >0)
						{
							NTemp  = new BigDecimal(EANMem).subtract( new BigDecimal( EANAbs));
							EAP = NTemp.divide(new BigDecimal(EANMem),4,3); 
							EAPO= new BigDecimal(EANMem).subtract( new BigDecimal( EANAbo)).divide(new BigDecimal(EANMem),4,3);
							EAN = NTemp.divide(new BigDecimal(days), 1, RoundingMode.UP);	 
							grid.set(FIELD_ELL_ADA_PER, EAP);
							grid.set(FIELD_ELL_ADA_PER_OSS, EAPO);
							grid.set(FIELD_ELL_MEM, new BigDecimal(EANMem));
						}
						else
						{ 
							grid.set(FIELD_ELL_ADA_PER, null);
							grid.set(FIELD_ELL_ADA_PER_OSS, null);
							grid.set(FIELD_ELL_MEM, new BigDecimal(EANMem));
						}
	
						// Homeless Students ADA
						if (HANMem >0)
						{
							NTemp  = new BigDecimal(HANMem).subtract( new BigDecimal( HANAbs));
							HAP = NTemp.divide(new BigDecimal(HANMem),4,3); 
							HAPO= new BigDecimal(HANMem).subtract( new BigDecimal( HANAbo)).divide(new BigDecimal(HANMem),4,3);
							HAN = NTemp.divide(new BigDecimal(days), 1, RoundingMode.UP);  
							grid.set(FIELD_HOM_ADA_PER, HAP);
							grid.set(FIELD_HOM_ADA_PER_OSS, HAPO);
							grid.set(FIELD_HOM_MEM, new BigDecimal(HANMem));
						}
						else
						{ 
							grid.set(FIELD_HOM_ADA_PER, null);
							grid.set(FIELD_HOM_ADA_PER_OSS, null);	
							grid.set(FIELD_HOM_MEM, new BigDecimal(HANMem));
						}
						BigDecimal AvgMem = new BigDecimal(AANMem).divide(new BigDecimal(days), 1, RoundingMode.UP);
						BigDecimal AvgMemD = new BigDecimal(DANMem).divide(new BigDecimal(days), 1, RoundingMode.UP);
						BigDecimal AvgMemE = new BigDecimal(EANMem).divide(new BigDecimal(days), 1, RoundingMode.UP);
						BigDecimal AvgMemH = new BigDecimal(HANMem).divide(new BigDecimal(days), 1, RoundingMode.UP);
						
			if (AvgMem.compareTo(BigDecimal.ZERO)>0)
			{
			ABP = new BigDecimal(ABN ).divide(new BigDecimal(studentCount),4,3); 
			A10 = new BigDecimal(A10N).divide(new BigDecimal(studentCount),4,3);
			A10O = new BigDecimal(A10NO).divide(new BigDecimal(studentCount),4,3);
			}
			grid.set(FIELD_ALL_ADA_NUM, studentCount);
			grid.set(FIELD_ALL_ABS_NUM, ABN);
			grid.set(FIELD_ALL_ABS_PER, ABP); 
			grid.set(FIELD_ALL_OS, AOS); 
			grid.set(FIELD_ALL_10, A10);
			grid.set(FIELD_ALL_10_NUM, A10N);
			grid.set(FIELD_ALL_10_OSS, A10O);
			grid.set(FIELD_ALL_10_OSS_NUM, A10NO);
	
	
			
			grid.set(FIELD_DIS_OS, 		DOS);  
			grid.set(FIELD_ELL_OS, 		EOS); 
			grid.set(FIELD_HOM_OS, 		HOS); 
	
			if (AvgMemD.compareTo(BigDecimal.ZERO)>0){
				DBP = new BigDecimal(DBN ).divide(new BigDecimal(disCount),4,3); 
				D10 = new BigDecimal(D10N).divide(new BigDecimal(disCount),4,3);
				D10O = new BigDecimal(D10NO).divide(new BigDecimal(disCount),4,3);
				grid.set(FIELD_DIS_ADA_NUM, disCount);
				grid.set(FIELD_DIS_ABS_NUM, DBN);
				grid.set(FIELD_DIS_ABS_PER, DBP); 
				grid.set(FIELD_DIS_10, D10);
				grid.set(FIELD_DIS_10_OSS, D10O);
	
			}
			else
			{
				grid.set(FIELD_DIS_ADA_NUM, 0);
				grid.set(FIELD_DIS_ABS_NUM, null);
				grid.set(FIELD_DIS_ABS_PER, null); 
				grid.set(FIELD_DIS_10, null);
				grid.set(FIELD_DIS_10_OSS, null);
	
			}
	
			if (AvgMemE.compareTo(BigDecimal.ZERO)>0){
				EBP = new BigDecimal(EBN ).divide(new BigDecimal(EllCount),4,3); 
				E10 = new BigDecimal(E10N).divide(new BigDecimal(EllCount),4,3);
				E10O = new BigDecimal(E10NO).divide(new BigDecimal(EllCount),4,3);
				grid.set(FIELD_ELL_ADA_NUM, EllCount);
				grid.set(FIELD_ELL_ABS_NUM, EBN);
				grid.set(FIELD_ELL_ABS_PER, EBP); 
				grid.set(FIELD_ELL_10, E10);
				grid.set(FIELD_ELL_10_OSS, E10O);
			}
			else
			{
				grid.set(FIELD_ELL_ADA_NUM, 0);
				grid.set(FIELD_ELL_ABS_NUM, null);
				grid.set(FIELD_ELL_ABS_PER, null); 
				grid.set(FIELD_ELL_10, null);
				grid.set(FIELD_ELL_10_OSS, null);
	
			}
	
			if (HomeCount >0){
				HBP = new BigDecimal(HBN ).divide(new BigDecimal(HomeCount),4,3);    
				H10 = new BigDecimal(H10N).divide(new BigDecimal(HomeCount),4,3);
				H10O = new BigDecimal(H10NO).divide(new BigDecimal(HomeCount),4,3);
				grid.set(FIELD_HOM_ADA_NUM, HomeCount);
				grid.set(FIELD_HOM_ABS_NUM, HBN);
				grid.set(FIELD_HOM_ABS_PER, HBP); 
				grid.set(FIELD_HOM_10, H10);
				grid.set(FIELD_HOM_10_OSS, H10O);
			}
			else
			{
				grid.set(FIELD_HOM_ADA_NUM, 0);
				grid.set(FIELD_HOM_ABS_NUM, null);
				grid.set(FIELD_HOM_ABS_PER, null); 
				grid.set(FIELD_HOM_10, null);
				grid.set(FIELD_HOM_10_OSS, null);
	
			}
		 } 
		}
		} // for each Homerooms
	
	}
	
	 
	
	/**
	 * Returns a sub query for the list of students to include in the query.
	 * 
	 * @param beanPath
	 * 
	 * @return SubQuery;
	 */
	private SubQuery getSubQueryFromSelection(String beanPath)
	{ 
	
		SubQuery studentSubQuery = null;
	
		Criteria subCriteria = new Criteria();
		subCriteria.addEqualTo(SelectionObject.COL_SELECTION_OID, m_selection.getOid());
		subCriteria.addEqualToField(SelectionObject.COL_OBJECT_OID, Criteria.PARENT_QUERY_PREFIX
				+ beanPath);
	
		studentSubQuery = new SubQuery(SelectionObject.class, X2BaseBean.COL_OID, subCriteria); 
		return studentSubQuery;
	
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
		
		if (queryString!= null && queryString.equals("##current") && !m_districtSummary)
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
	
		SubQuery schoolSubQuery = new SubQuery(SisSchool.class, SisSchool.COL_OID, subCriteria);
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
	
	private void ProcessMembership(final PairedEnrollment enrollment, String sOid )
	{
		if (m_schoolCalendars.containsKey(enrollment.SchoolOid))
		{
		String mkey = m_schoolCalendars.get(enrollment.SchoolOid).values().iterator().next().getOid();
		
		Collection<SchoolCalendarDate> schoolDays =   m_calendarDays.get(mkey);
	
		HashMap<PlainDate, StudentAttendance> absences = null;
		if (m_absences.containsKey(sOid))
		{
			absences =m_absences.get(sOid);
		}
		
		 
		if (schoolDays != null)
		{
		for (SchoolCalendarDate SDay: schoolDays)
		{
			PlainDate date = SDay.getDate();
			if (date.compareTo(m_startDate) >=0 && date.compareTo(m_endDate ) <=0
					&& ( enrollment.RegistrationDate  != null && date.compareTo(enrollment.RegistrationDate ) >=0)  
					&& (enrollment.WithdrawDate == null || (enrollment.WithdrawDate != null && date.compareTo(enrollment.WithdrawDate ) <=0)))
			{
				StudentAttendance Abs = null;
				enrollment.Membership_days = enrollment.Membership_days +1;
				if (absences != null && absences.containsKey(date))
				{
					Abs = absences.get(date);
				}
	
				if (Abs != null)
				{
					if(Abs.getAbsentIndicator()) // if Absent
					{
						enrollment.Absences =enrollment.Absences  +1;
						if ( Abs.getExcusedIndicator() ) // If Excused
						{
							enrollment.Excused =enrollment.Excused  +1; 
						}
						else  //If not then increment Unexcused
						{
							enrollment.Unexcused =enrollment.Unexcused  +1; 
						}
	
						if ((Abs.getReasonCode()!= null && Abs.getReasonCode().equals(OS_CODE)) || (Abs.getCodeView()!= null &&Abs.getCodeView().equals("A-E OS")))
						{
							enrollment.OSS = enrollment.OSS+1;
						}
					}
					else
					{
						if (Abs.getTardyIndicator())
						{
							enrollment.Tardy = enrollment.Tardy  +1;
						}
					} 
				}
	
				 
			}
		} 
		}}
	}
	
	
	
	private HashMap<String,Collection<PairedEnrollment>> populateEnrollments()
	{
		HashMap<String,Collection<PairedEnrollment>> results = new HashMap<String,Collection<PairedEnrollment>>();
		m_schoolMemberships = new HashMap<String, Collection<PairedEnrollment >>();
		X2Criteria m_enrollmentCriteria = new X2Criteria();  
		m_enrollmentCriteria.addNotEqualTo(StudentEnrollment.COL_ENROLLMENT_TYPE, "Y");
		m_enrollmentCriteria.addExists(getSubQueryFromSelection(StudentEnrollment.COL_STUDENT_OID ));
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
	
	protected void getTruancy()
	{
	m_excludeWithdrawnStudents = true;
	String excludedGrades = GRADE_PK;
	m_excludedGrades = Arrays.asList(excludedGrades.split("\\s*,\\s*"));
	
	/*  m_schoolOids = (String) getParameter(PARAMETER_SCHOOL_OIDS);
	if (!StringUtils.isEmpty(m_schoolOids)) {
		m_schoolOidsList = Arrays.asList(m_schoolOids.split(","));
	} */
	
	ReportDataGrid grid = new ReportDataGrid(INITIAL_STUDENT_CAPACITY, 15);
	 
	  
	for (SisSchool  school : m_schools) {
		// Reset school variables 
		m_truantStdsCount = 0; 
		m_truantStdsCountD = 0;
		m_truantStdsCountE = 0;
		m_truantStdsCountH = 0;
		m_ttlDaysInSession = 0;
		m_sumDaysEnrolled = 0; 
		m_avgDailyMembership = 0;
		m_truancyRate = 0;
		m_truancyRateE = 0;
		m_truancyRateD = 0;
		m_truancyRateH = 0;
		m_sumDaysEnrolledE=0;
		m_sumDaysEnrolledD=0;
		m_sumDaysEnrolledH=0;
		m_avgDailyMembershipE =0;
		m_avgDailyMembershipD =0;
		m_avgDailyMembershipH =0;
				
	 
	    
	    loadEnrollmentData(school);
	    loadAttendanceData(school);
	    
	    QueryByCriteria studentQuery = new QueryByCriteria(SisStudent.class, getStudentCriteria(school));
	    if (includeSecondaryStudents())
	    {
	        studentQuery.setDistinct(true);
	    }
	    studentQuery.addOrderByAscending(SisStudent.COL_GRADE_LEVEL);
	    studentQuery.addOrderByAscending(SisStudent.COL_NAME_VIEW);
	    
	    QueryIterator students = null;
	    try
	    {
	        students = getBroker().getIteratorByQuery(studentQuery);
	        while (students.hasNext())
	        {
	            SisStudent student = (SisStudent) students.next();
	            
	            populateEnrollmentCounts(student);
	            populateAttendanceCounts(grid, student);
	            
	            if (m_daysUnexcused >= m_truancyThreshold) 
	            {
	            	if (student.getPerson().getAgeAsOfDate(m_endDate) < m_ageLimit ) {
	                	if (checkForValidStudentData(grid, student) ) { 
	                    	m_truantStdsCount++;
	                    	if (student.getSpedStatusCode()  != null &&  student.getSpedStatusCode().equals(STATUS_ACTIVE) ) m_truantStdsCountD++;
	                        if (m_ESL.containsKey(student.getOid())) m_truantStdsCountE++;
	                        if (m_Homeless.containsKey(student.getOid())) m_truantStdsCountH++;
	                	}
	            	}
	            }
	            if (student.getSpedStatusCode()  != null &&  student.getSpedStatusCode().equals(STATUS_ACTIVE) )  m_sumDaysEnrolledD = m_sumDaysEnrolledD+ m_daysEnrolled;
	            if (m_ESL.containsKey(student.getOid())) m_sumDaysEnrolledE = m_sumDaysEnrolledE+ m_daysEnrolled;
	            if (m_Homeless.containsKey(student.getOid())) m_sumDaysEnrolledH = m_sumDaysEnrolledH+ m_daysEnrolled;
	        }
	    }
	    finally
	    {
	        if (students != null)
	        {
	            students.close();
	        }
	    }
	             
	    m_schoolDaysCount = schoolDayCount(school); 
	    
	    if (m_schoolDaysCount >0)
	    {	
	    m_avgDailyMembership = m_sumDaysEnrolled / m_schoolDaysCount;
	    if (m_avgDailyMembership >0)   m_truancyRate = m_truantStdsCount / m_avgDailyMembership;
	    else m_truancyRate=0  ;
	    
	    m_avgDailyMembershipE = m_sumDaysEnrolledE / m_schoolDaysCount;
	    if (m_avgDailyMembershipE >0)   m_truancyRateE = m_truantStdsCountE / m_avgDailyMembershipE;
	    else m_truancyRateE=0  ;
	    
	    m_avgDailyMembershipD = m_sumDaysEnrolledD / m_schoolDaysCount;
	    if (m_avgDailyMembershipD >0)   m_truancyRateD = m_truantStdsCountD / m_avgDailyMembershipD;
	    else m_truancyRateD=0  ;
	    
	    m_avgDailyMembershipH = m_sumDaysEnrolledH / m_schoolDaysCount;
	    if (m_avgDailyMembershipH >0)   m_truancyRateH = m_truantStdsCountH / m_avgDailyMembershipH;
	    else m_truancyRateH=0  ;
	    
	    if (m_avgDailyMembership > 0) 
	    {
	    	m_truancy.put(school.getOid(), new BigDecimal(m_truancyRate));
	    	m_truancyD.put(school.getOid(), new BigDecimal(m_truancyRateD)); 
	    	m_truancyE.put(school.getOid(), new BigDecimal(m_truancyRateE)); 
	    	m_truancyH.put(school.getOid(), new BigDecimal(m_truancyRateH)); 
	    }
	    }
	}
	 
	}
	
	
	/**
	 * Removes student from the grid based on various data checks:
	 * 1. Did the student withdraw prior to the start date of the report?
	 * 2. Does the student have zero or negative days enrolled during the date range?
	 * 3. Is the student inactive, but have a "E" as the most recent enrollment record?
	 * 
	 * @param grid
	 * @param student
	 */
	private boolean checkForValidStudentData(ReportDataGrid grid, SisStudent student) {
	    // Test 1: Student is not valid for this report if withdrew prior to start date
	    if (m_reportStatus==REPORT_STATUS_WITHDREW_PRIOR) {
	    	return false;
	    }
	    // Test 2: Student is not valid for this report if days enrolled during date range is zero or negative
	    else if (m_daysEnrolled <= 0) {
	    	return false;
	    }
	    // Test 3: If user has selected to exclude students withdrawn during the date range, then student MUST be active as of end date
	    else if (m_excludeWithdrawnStudents && (m_reportStatus==REPORT_STATUS_DROPPED || m_reportStatus==REPORT_STATUS_ADDED_DROPPED)) {
	    	return false;
	    }   
	    else if (!m_activeCode.equalsIgnoreCase(student.getEnrollmentStatus())) {
	    	// Test 4: Student is not valid for this report if enrollment status is not active, and no enrollment record exists in grid
	    	if (m_enrollment==null) {
	    		return false;
	    	}
	    	// Test 5: Student is not valid for this report if enrollment status is not active, but most recent enrollment record is an E
	    	else if (StudentEnrollment.ENTRY.equals(m_enrollment.getEnrollmentType())) {
	    		return false;
	    	}
	    }
	    return true;
	}
	
	/**
	 * @see com.follett.fsc.core.k12.tools.ToolJavaSource#initialize()
	 */
	@Override
	protected void initialize() throws X2BaseException
	{
	    super.initialize();
	    m_activeCode = PreferenceManager.getPreferenceValue(getOrganization(), SystemPreferenceDefinition.STUDENT_ACTIVE_CODE);
	    m_calendar = Calendar.getInstance(getLocale());
	    m_calendarManager = new CalendarManager(getBroker());
	    m_enrollmentManager = new EnrollmentManager(getBroker(), getPrivilegeSet(), getOrganization());
	}
	
	/**
	 * @see com.follett.fsc.core.k12.tools.reports.ReportJavaSourceDori#releaseResources()
	 */
	@Override
	protected void releaseResources()
	{
	    super.releaseResources();
	    
	    m_attendanceData.clear();
	    m_attendanceData = null;
	    
	    m_calendarData.clear();
	    m_calendarData = null;
	    
	    m_enrollmentData.clear();
	    m_enrollmentData = null;
	    
	    m_priorEnrollments.clear();
	    m_priorEnrollments = null;
	}
	
	/**
	 * Loads enrollment data required by this report including the map of calendar in session days
	 * and the map of enrollment data grouped by student OID.
	 * @param school
	 */
	private void loadEnrollmentData(SisSchool school)
	{        
	  DistrictSchoolYearContext context = m_calendarManager.getDistrictContext(m_endDate, getOrganization().getOid());
	  if (context != null)
	  {
	      m_calendarData = m_enrollmentManager.getCalendarLookup(school, m_startDate, m_endDate, context.getOid());
	  }
	  else
	  {
	      m_calendarData = m_enrollmentManager.getCalendarLookup(school, m_startDate, m_endDate, school.getOrganization1().getCurrentContextOid());
	  }
	
	  // Get the set of OIDs identifying students enrolled on the start date
	  m_priorEnrollments = m_enrollmentManager.getMembershipAsOf(m_startDate, school);
	  
	  Criteria criteria = new Criteria();
	  criteria = getEnrollmentCriteria(getSchoolOids(school));
	  
	  // DATA FIX :: Eliminate students with a YOG in the past
	  //criteria.addGreaterOrEqualThan(StudentEnrollment.REL_STUDENT + PATH_DELIMITER + SisStudent.COL_YOG, m_currYear);
	
	  QueryByCriteria query = 
	      new QueryByCriteria(StudentEnrollment.class, criteria);
	  query.addOrderByAscending(StudentEnrollment.COL_ENROLLMENT_DATE);
	  query.addOrderByAscending(StudentEnrollment.COL_TIMESTAMP);
	  
	  m_enrollmentData = 
	      getBroker().getGroupedCollectionByQuery(query, StudentEnrollment.COL_STUDENT_OID, 
	              INITIAL_STUDENT_CAPACITY);
	}
	
	/** 
	 * Modified to allow entire report to run for the entire district
	 * 
	 * @see com.follett.fsc.core.k12.tools.reports.SecondaryStudentDataSource#getSchoolOids()
	 */
	private Collection<String> getSchoolOids(SisSchool school)
	{
	  HashSet<String> schoolOids = new HashSet();
	  schoolOids.add(school.getOid());
	  if (includeSecondaryStudents())
	  {
	    Criteria criteria = StudentManager.buildSecondaryStudentCriteria(school);
	    SubQuery subQuery = new SubQuery(SisStudent.class, "schoolOid", criteria);
	    Collection otherSchoolOids = getBroker().getSubQueryCollectionByQuery(subQuery);
	    
	    schoolOids.addAll(otherSchoolOids);
	  }
	  return schoolOids;
	}
	
	/**
	 * Loads the map of attendance beans grouped by student OID.
	 * @param school
	 */
	private void loadAttendanceData(SisSchool school)
	{
	    Criteria criteria = new Criteria();
	    criteria.addGreaterOrEqualThan(StudentAttendance.COL_DATE, m_startDate);
	    criteria.addLessOrEqualThan(StudentAttendance.COL_DATE, m_endDate);
	    
	    
	    QueryByCriteria attendanceQuery = new QueryByCriteria(StudentAttendance.class, criteria);
	    attendanceQuery.addOrderByAscending(SisSchoolCalendarDate.COL_DATE);
	    
	    m_attendanceData = getBroker().getGroupedCollectionByQuery(attendanceQuery, 
	            StudentAttendance.COL_STUDENT_OID, INITIAL_STUDENT_CAPACITY);
	}
	
	/**
	 * Returns a Criteria that finds all enrollment records containing schools being reported
	 * for the report date range.
	 * 
	 * @param schoolOids
	 * 
	 * @return Criteria
	 */
	private Criteria getEnrollmentCriteria(Collection schoolOids)
	{
	    Criteria criteria = new Criteria();
	    criteria.addGreaterOrEqualThan(StudentEnrollment.COL_ENROLLMENT_DATE, m_startDate);
	    criteria.addLessOrEqualThan(StudentEnrollment.COL_ENROLLMENT_DATE, m_endDate);
	    criteria.addIn(StudentEnrollment.COL_SCHOOL_OID, schoolOids);
	    criteria.addNotIn(StudentEnrollment.REL_STUDENT + PATH_DELIMITER + SisStudent.COL_GRADE_LEVEL, m_excludedGrades);
	    
	    X2Criteria typeCriteria = new X2Criteria();
	    typeCriteria.addEqualTo(StudentEnrollment.COL_ENROLLMENT_TYPE, StudentEnrollment.ENTRY);
	    typeCriteria.addOrEqualTo(StudentEnrollment.COL_ENROLLMENT_TYPE, StudentEnrollment.WITHDRAWAL);
	    
	    criteria.addAndCriteria(typeCriteria);
	    
	    return criteria;
	}
	
	/**
	 * Returns a Criteria that finds all students for this report.
	 * <ul>
	 * <li>The student's school OID is that of the school being reported, OR
	 * <li>An enrollment record exists during the date range for the student containing the current
	 * school OID being reported OR
	 * <li>Any secondary students for the school if records for secondary students are included
	 * <ul>
	 * 
	 * @param school
	 * 
	 * @return Criteria
	 */
	private Criteria getStudentCriteria(SisSchool school)
	{
	    Criteria studentCriteria = new Criteria();
	    studentCriteria.addEqualTo(SisStudent.COL_SCHOOL_OID, school.getOid());
	    studentCriteria.addNotIn(SisStudent.COL_GRADE_LEVEL, m_excludedGrades);
	    
	    // DATA FIX :: Exclude students with YOG in the past and YOG = 0
	   // studentCriteria.addGreaterOrEqualThan(SisStudent.COL_YOG, m_currYear);
	    
	    Collection<String> schoolOids = new ArrayList<String>();
	    schoolOids.add(school.getOid());
	    
	    Criteria enrollmentCriteria = new Criteria();
	    enrollmentCriteria.addIn(X2BaseBean.COL_OID, new SubQuery(StudentEnrollment.class, 
	            StudentEnrollment.COL_STUDENT_OID, getEnrollmentCriteria(schoolOids)));
	
	    studentCriteria.addOrCriteria(enrollmentCriteria);
	    
	    /*
	     * Check if secondary students are included.
	     */
	    if (includeSecondaryStudents())
	    {
	        studentCriteria.addOrCriteria(StudentManager.buildSecondaryStudentCriteria(school));
	    }
	    
	    return studentCriteria;
	}
	
	/**
	 * Sets the counts of days enrolled and days not enrolled for the current student in the passed
	 * grid.
	 *
	 * @param grid
	 */
	private void populateEnrollmentCounts(SisStudent student)
	{
		// Reset student variables
		m_daysEnrolled = 0;
		m_daysInSession = 0;
		m_enrollment = null;
	    m_reportStatus = 0;
	    
	    boolean enrolled = m_priorEnrollments.contains(student.getOid());
	    if (!enrolled)
	    {
	        m_reportStatus = REPORT_STATUS_WITHDREW_PRIOR;
	    }
	
	    HashSet entryDates = new HashSet(5);
	    HashSet withdrawalDates = new HashSet(5);
	    Set sessionDates = (Set) m_calendarData.get(student.getCalendarCode());
	    if (sessionDates == null)
	    {
	        sessionDates = new HashSet();
	    }
	    
	    List enrollments = (List) m_enrollmentData.get(student.getOid());
	    if (enrollments != null)
	    {
	        StudentEnrollment entry = null;
	        StudentEnrollment withdrawal = null;
	        Iterator enrollmentIterator = enrollments.iterator();
	        while (enrollmentIterator.hasNext())
	        {
	            m_enrollment = (StudentEnrollment) enrollmentIterator.next();
	            if (StudentEnrollment.ENTRY.equals(m_enrollment.getEnrollmentType()))
	            {
	                entryDates.add(m_enrollment.getEnrollmentDate());
	                if (m_reportStatus == REPORT_STATUS_DROPPED || m_reportStatus == REPORT_STATUS_ADDED_DROPPED)
	                {
	                    m_reportStatus = REPORT_STATUS_DROPPED_ADDED;
	                }
	                else
	                {
	                    m_reportStatus = REPORT_STATUS_ADDED;
	                }
	                entry = m_enrollment;
	            }
	            else if (StudentEnrollment.WITHDRAWAL.equals(m_enrollment.getEnrollmentType()))
	            {
	                withdrawalDates.add(m_enrollment.getEnrollmentDate());
	                if (m_reportStatus == REPORT_STATUS_ADDED || m_reportStatus == REPORT_STATUS_DROPPED_ADDED)
	                {
	                    m_reportStatus = REPORT_STATUS_ADDED_DROPPED;
	                }
	                else
	                {
	                    m_reportStatus = REPORT_STATUS_DROPPED;
	                }
	                withdrawal = m_enrollment;
	            }
	        }
	        
	    }
	    
	    m_calendar.setTimeInMillis(m_startDate.getTime());
	    while (!m_calendar.getTime().after(m_endDate))
	    {
	        boolean entryDate = entryDates.contains(m_calendar.getTime());
	        boolean withdrawalDate = withdrawalDates.contains(m_calendar.getTime());
	        
	        boolean inSession = sessionDates.contains(m_calendar.getTime());
	        
	        if (entryDate)
	        {
	            if (m_enrollmentManager.getEntryIsMemberDay() && inSession)
	            {
	                m_daysEnrolled++;
	            }
	            enrolled = true;
	        }
	        if (withdrawalDate)
	        {
	            if (m_enrollmentManager.getWithdrawalIsMemberDay() && inSession)
	            {
	                m_daysEnrolled++;
	            }
	            enrolled = false;
	        }
	        if (enrolled && !entryDate && !withdrawalDate && inSession)
	        {
	            m_daysEnrolled++;
	        }
	        
	        if (inSession)
	        {
	            m_daysInSession++;
	        }
	        
	        m_calendar.add(Calendar.DAY_OF_YEAR, 1);
	    }
	     
		m_ttlDaysInSession = m_ttlDaysInSession + m_daysInSession;
		m_sumDaysEnrolled = m_sumDaysEnrolled + m_daysEnrolled;
	}
	
	/**
	 * Sets the counts of days present, excused, and unexcused for the current student in the passed
	 * grid.
	 *
	 * @param grid
	 */
	private void populateAttendanceCounts(ReportDataGrid grid, SisStudent student)
	{
	    Set sessionDates = (Set) m_calendarData.get(student.getCalendarCode());
	    if (sessionDates == null)
	    {
	        sessionDates = new HashSet();
	    }
	    
	    // Reset student variables
	    m_notPresent = 0;
	    m_notPresentExcused = 0;
	    m_daysUnexcused = 0;
	    
	    List attendanceList = (List) m_attendanceData.get(student.getOid());
	    if (attendanceList != null)
	    {
	        Iterator attendanceIterator = attendanceList.iterator();
	        while (attendanceIterator.hasNext())
	        {
	            StudentAttendance attendance = (StudentAttendance) attendanceIterator.next();
	            
	            if (sessionDates.contains(attendance.getDate()))
	            {
	                if (attendance.getAbsentIndicator())
	                {
	                    double portionAbsent = attendance.getPortionAbsent() != null ? 
	                            attendance.getPortionAbsent().doubleValue() : 0; 
	                            
	                    m_notPresent += portionAbsent;
	                    
	                    if (attendance.getExcusedIndicator())
	                    {
	                        m_notPresentExcused += portionAbsent;
	                    }
	                }
	            }
	        }
	    }
	    
	    m_daysUnexcused = m_notPresent - m_notPresentExcused;
	}
	
	/**
	 * Adds a report parameter containing the number of school days to be displayed on the header
	 * of the report. The calendar used to determine the count is the calendar associated with the 
	 * most students on the report. Since only one count appears, it is possible that this number 
	 * may not apply to some students associated with different calendars.
	 * 
	 * @param school
	 */
	private int schoolDayCount(SisSchool school)
	{
	    int dayCount = 0;
	    
	    String calendarId = null;
	    
	    String countColumn = "count(" + SisStudent.COL_CALENDAR_CODE + ") as cnt "; 
	    
	    ReportQueryByCriteria calendarIdQuery = new ReportQueryByCriteria(SisStudent.class, 
	            new String[] { SisStudent.COL_CALENDAR_CODE, countColumn },
	            getStudentCriteria(school));
	    calendarIdQuery.addGroupBy(SisStudent.COL_CALENDAR_CODE);
	    // This works with OJB because the column is converted to a number in the SQL order by clause
	    calendarIdQuery.addOrderByDescending(countColumn); 
	    
	    ReportQueryIterator calendarIdIterator = null; 
	    try
	    {
	        calendarIdIterator = getBroker().getReportQueryIteratorByQuery(calendarIdQuery);
	        if (calendarIdIterator.hasNext())
	        {
	            Object[] row = (Object[]) calendarIdIterator.next();
	            calendarId = (String) row[0]; 
	        }
	    }
	    finally
	    {
	        if (calendarIdIterator != null)
	        {
	            calendarIdIterator.close();
	        }
	    }
	    
	    if (calendarId != null)
	    {
	        Criteria dayCountCriteria = new Criteria();
	        dayCountCriteria.addEqualTo(SisSchoolCalendarDate.REL_SCHOOL_CALENDAR + "." + 
	                SchoolCalendar.COL_SCHOOL_OID, school.getOid());
	        dayCountCriteria.addEqualTo(SisSchoolCalendarDate.REL_SCHOOL_CALENDAR + "." +
	                SchoolCalendar.COL_CALENDAR_ID, calendarId);
	        dayCountCriteria.addGreaterOrEqualThan(SisSchoolCalendarDate.COL_DATE, m_startDate);
	        dayCountCriteria.addLessOrEqualThan(SisSchoolCalendarDate.COL_DATE, m_endDate);
	        dayCountCriteria.addEqualTo(SisSchoolCalendarDate.COL_IN_SESSION_INDICATOR, Boolean.TRUE);
	        
	        QueryByCriteria query = new QueryByCriteria(SisSchoolCalendarDate.class, dayCountCriteria);
	        
	        dayCount = getBroker().getCount(query);
	    }
	    
	    return dayCount;
	} 
	 
		
		public void LoadHomelessStatus()
		{
			Criteria criteria = new Criteria();
			Criteria combo = new Criteria();
			Criteria one = new Criteria();
			Criteria two = new Criteria();
			
			one.addGreaterOrEqualThan(StudentProgramParticipation.COL_END_DATE, m_endDate);
			two.addIsNull(StudentProgramParticipation.COL_END_DATE);
			combo.addOrCriteria(one);
			combo.addOrCriteria(two);
			
			criteria.addLessOrEqualThan(StudentProgramParticipation.COL_START_DATE, m_endDate);
			criteria.addAndCriteria(combo);
			criteria.addEqualTo(StudentProgramParticipation.COL_PROGRAM_CODE , CONSTANT_HOMELESS);
			QueryByCriteria query =  new QueryByCriteria(StudentProgramParticipation.class, criteria);
			m_Homeless =  getBroker().getMapByQuery(query, StudentProgramParticipation.COL_STUDENT_OID, 1000);
		}
	 
		
		public void LoadESLStatus()
		{
			Criteria criteria = new Criteria();
			Criteria combo = new Criteria();
			Criteria one = new Criteria();
			Criteria two = new Criteria();
			
			one.addGreaterOrEqualThan(StudentProgramParticipation.COL_END_DATE, m_endDate);
			two.addIsNull(StudentProgramParticipation.COL_END_DATE);
			combo.addOrCriteria(one);
			combo.addOrCriteria(two);
			
			criteria.addLessOrEqualThan(StudentProgramParticipation.COL_START_DATE, m_endDate);
			criteria.addAndCriteria(combo);
			criteria.addEqualTo(StudentProgramParticipation.COL_PROGRAM_CODE , CONSTANT_ESL);
			QueryByCriteria query =  new QueryByCriteria(StudentProgramParticipation.class, criteria);
			m_ESL =  getBroker().getMapByQuery(query, StudentProgramParticipation.COL_STUDENT_OID, 1000);
		}
	 
		
		public Map<String, Collection<SisStudent>> LoadRASStudents()
		{
			Criteria criteria = new Criteria();
			/*if (isSchoolContext())
			{
				criteria.addEqualTo(SisStudent.COL_SCHOOL_OID, getSchool().getOid());
			}  */
			
			
			Criteria criteriaA = new Criteria();
			Criteria criteriaB = new Criteria();
			Criteria criteriaC = new Criteria();
			Criteria criteriaD = new Criteria();
			Criteria comboCriteria = new Criteria();
			Criteria comboCriteria2 = new Criteria();
	
			// Active student
			criteriaA.addEqualTo(SisStudent.COL_ENROLLMENT_STATUS, STATUS_ACTIVE);
			criteriaA.addEqualTo(SisStudent.COL_SCHOOL_OID,  RICHMOND_ALTERNATIVE);
	
	 
	
			criteria.addAndCriteria(criteriaA);  //comboCriteria
			
			criteriaC.addEqualTo(  SisStudent.COL_FIELD_A032 , "0");
			criteriaD.addEqualTo(  SisStudent.COL_FIELD_A032 , null);
			comboCriteria2.addOrCriteria(criteriaC);
			comboCriteria2.addOrCriteria(criteriaD);
			
			criteria.addAndCriteria(comboCriteria2);  //comboCriteria
	
			QueryByCriteria query =  new QueryByCriteria(SisStudent.class, criteria);
			query.setDistinct(true);
	
			return    getBroker().getGroupedCollectionByQuery(query, SisStudent.COL_HOMEROOM, 100); 
		 
		}
	  
		//   Code for Mobility Rate
		
		protected void getMobilityRates()
		{
			
			if (m_endDate.compareTo(m_startDate) < 0)
			{
				m_endDate = m_startDate;
			}
			 
			getAmeliaSchools();
			LoadEnrollments();
			loadStudents();
			getSchoolSubQueryEnr( );
			
			ProcessEnrollments( ); 
			
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
			criteria.addNotEqualTo(StudentEnrollment.REL_STUDENT + PATH_DELIMITER + SisStudent.COL_GRADE_LEVEL, "UN");
			criteria.addNotEqualTo(StudentEnrollment.REL_STUDENT + PATH_DELIMITER + SisStudent.COL_GRADE_LEVEL, "IN");
			 
			criteria.addIn(StudentEnrollment.COL_SCHOOL_OID , m_schoolSub);
			 
			QueryByCriteria query = new QueryByCriteria(StudentEnrollment.class, criteria);
	
			m_enrollments =	getBroker().getGroupedCollectionByQuery(query  , StudentEnrollment.COL_SCHOOL_OID, 100); 
		}
	  
	 
		
	
		/**
		 * Returns a sub query for the list of Schools to include in the query.
		 * 
		 * @param none
		 * 
		 * @return SubQuery;
		 */
		private SubQuery getSchoolSubQueryEnr( )
		{ 
	 
			Criteria subCriteria = new Criteria();  
			Collection<String> AmeliaSchools = new ArrayList<String>(Arrays.asList(AMELIA_SCHOOLS));
			
			if (!m_districtSummary && isSchoolContext()){
				if (getSchool().getSchoolId().equals("307"))
				{
					subCriteria.addIn(SisSchool.COL_SCHOOL_ID, AmeliaSchools);
				}
				else
				{
				subCriteria.addEqualTo(SisSchool.COL_OID, getSchool().getOid());
				}
			}
			else if (!m_districtSummary && m_currentSchool != null )
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
			
			if (!m_districtSummary && queryString!= null && queryString.equals("##current"))
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
			//m_schools_enr = getBroker().getCollectionByQuery(schoolQuery);
			return schoolSubQuery; 
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
			StuCriteria.addNotEqualTo(  SisStudent.COL_GRADE_LEVEL, "IN");
			StuCriteria.addNotEqualTo(  SisStudent.COL_GRADE_LEVEL, "UN");
			//StuCriteria.addEqualTo( SisStudent.COL_FIELD_A032 , false);
			QueryByCriteria stuQuery = new QueryByCriteria(SisStudent.class, StuCriteria);
			m_students_enr = getBroker().getMapByQuery(stuQuery, SisStudent.COL_OID, 1000); 
		}
		
		public void getAmeliaSchools()
		{
			Criteria criteria = new Criteria();
			Collection<String> AmeliaSchools = new ArrayList<String>(Arrays.asList(AMELIA_SCHOOLS_SUB));
			criteria.addIn(SisSchool.COL_SCHOOL_ID , AmeliaSchools);
			
			QueryByCriteria query =  new QueryByCriteria(SisSchool.class, criteria);
			m_AmSchools =   getBroker().getCollectionByQuery(query);
		}
		
		private void ProcessEnrollments( )
		{
			m_mobilityRates = new HashMap <String, BigDecimal[]>();
			
			 if (!m_districtSummary )
			 {
			Collection<String> excludeER  = new ArrayList<String>(Arrays.asList(EXCLUDE_CODES_EN));
			Collection<String> excludeWD = new ArrayList<String>(Arrays.asList(EXCLUDE_CODES_WD)); 
			for( SisSchool School : m_schools)
			{
				 
				String schoolOid = School.getOid();  
				
				if (School != null)
				{ 
					
				  
				Collection<String>  AllTotal = new HashSet<String>(); 
				Collection<String>  ELLTotal = new HashSet<String>(); 
				Collection<String>  DisTotal = new HashSet<String>(); 
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
					
						SisStudent student = m_students_enr.get(enrollment.getStudentOid()); 
					if (student.getFieldA039()==null || student.getFieldA039().equals("0"))	
					 {
						if (enrollment.getEnrollmentType().equals(CON_ADMIT)  )
						{
							if ( !excludeER.contains(enrollment.getEnrollmentCode() ))
							{ 
							AllTotal.add(enrollment.getStudentOid());
							}
						}
						else if (enrollment.getEnrollmentType().equals(CON_WITHDRAW) && !excludeWD.contains(enrollment.getEnrollmentCode() ))
						{ 
							AllTotal.add(enrollment.getStudentOid());
						}
						
						// LEP Student
						if (m_ESL.containsKey(student.getOid()))
						 { 
							if (enrollment.getEnrollmentType().equals(CON_ADMIT) )
							{
								if ( !excludeER.contains(enrollment.getEnrollmentCode() ))
								{ 
								ELLTotal.add(enrollment.getStudentOid());
								}
							}
							else if (enrollment.getEnrollmentType().equals(CON_WITHDRAW)&& !excludeWD.contains(enrollment.getEnrollmentCode() ))
							{ 
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
								DisTotal.add(enrollment.getStudentOid());
								}
							}
							else if (enrollment.getEnrollmentType().equals(CON_WITHDRAW )&& !excludeWD.contains(enrollment.getEnrollmentCode() ))
							{ 
								DisTotal.add(enrollment.getStudentOid());
							}
						 }
						// Homeless
						if (m_Homeless.containsKey(student.getOid()))
						 {
							if (enrollment.getEnrollmentType().equals(CON_ADMIT) )
							{
								if ( !excludeER.contains(enrollment.getEnrollmentCode() ))
								{ 
								HomTotal.add(enrollment.getStudentOid());
								}
							}
							else if (enrollment.getEnrollmentType().equals(CON_WITHDRAW) && !excludeWD.contains(enrollment.getEnrollmentCode() ))
							{ 
								HomTotal.add(enrollment.getStudentOid());
							}
						 }
						 }
					}				
				}
				}
				 
	
				BigDecimal[] mRates = new BigDecimal[4];
				
				mRates[0]= new BigDecimal ( AllTotal.size()); 
				mRates[1]= new BigDecimal ( DisTotal.size()); 
				mRates[2]= new BigDecimal ( ELLTotal.size()); 
				mRates[3]= new BigDecimal ( HomTotal.size());
				
				BigDecimal Field10B  = BigDecimal.ZERO;
				 
		 
			 if (School.getFieldA010() != null && !School.getFieldA010().isEmpty())
				{
					int Field10 = Integer.parseInt(  School.getFieldA010());
					  Field10B  =  new BigDecimal(Field10);   
					  
					  mRates[0] = mRates[0].divide(Field10B, 4,3);
					  mRates[1] = mRates[1].divide(Field10B, 4,3);
					  mRates[2] = mRates[2].divide(Field10B, 4,3);
					  mRates[3] = mRates[3].divide(Field10B, 4,3);
					 
					  m_mobilityRates.put(School.getOid() , mRates);
				}
			  
			  
				} 
				} 
			 }
			 else 
			 {
				 Collection<String> excludeER  = new ArrayList<String>(Arrays.asList(EXCLUDE_CODES_EN));
					Collection<String> excludeWD = new ArrayList<String>(Arrays.asList(EXCLUDE_CODES_WD)); 
					 
	 
					Collection<String>  AllTotal = new HashSet<String>(); 
					Collection<String>  ELLTotal = new HashSet<String>(); 
					Collection<String>  DisTotal = new HashSet<String>(); 
					Collection<String>  HomTotal = new HashSet<String>();
					BigDecimal OfficialEnrollment = BigDecimal.ZERO;
					for( SisSchool School : m_schools)
					{
						 
						String schoolOid = School.getOid();  
						
						if (School != null &&  School.getFieldA010() != null && !School.getFieldA010().isEmpty()) 
						{ 
						Collection<StudentEnrollment> Enrollments = m_enrollments.get(schoolOid);
						
						 
						if (Enrollments !=null && !Enrollments.isEmpty())
						{
						for (StudentEnrollment enrollment: Enrollments)
						{
							
							if (enrollment.getStudentOid() != null &&  !enrollment.getStudentOid().isEmpty() &&
									enrollment.getEnrollmentDate().compareTo(m_startDate) >=0 && enrollment.getEnrollmentDate().compareTo(m_endDate) <=0 
									&&(    enrollment.getFieldA013()== null ||    enrollment.getFieldA013().equals("0")  ))
							{
							
								SisStudent student = m_students_enr.get(enrollment.getStudentOid()); 
							if (student.getFieldA039()==null || student.getFieldA039().equals("0"))	
							{
								if (enrollment.getEnrollmentType().equals(CON_ADMIT)  )
								{
									if ( !excludeER.contains(enrollment.getEnrollmentCode() ))
									{ 
									AllTotal.add(enrollment.getStudentOid());
									}
								}
								else if (enrollment.getEnrollmentType().equals(CON_WITHDRAW) && !excludeWD.contains(enrollment.getEnrollmentCode() ))
								{ 
									AllTotal.add(enrollment.getStudentOid());
								}
								
								// LEP Student
								if (m_ESL.containsKey(student.getOid()))
								 { 
									if (enrollment.getEnrollmentType().equals(CON_ADMIT) )
									{
										if ( !excludeER.contains(enrollment.getEnrollmentCode() ))
										{ 
										ELLTotal.add(enrollment.getStudentOid());
										}
									}
									else if (enrollment.getEnrollmentType().equals(CON_WITHDRAW)&& !excludeWD.contains(enrollment.getEnrollmentCode() ))
									{ 
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
										DisTotal.add(enrollment.getStudentOid());
										}
									}
									else if (enrollment.getEnrollmentType().equals(CON_WITHDRAW )&& !excludeWD.contains(enrollment.getEnrollmentCode() ))
									{ 
										DisTotal.add(enrollment.getStudentOid());
									}
								 }
								// Homeless
								if ( m_Homeless.containsKey(student.getOid()) )
								 {
									if (enrollment.getEnrollmentType().equals(CON_ADMIT) )
									{
										if ( !excludeER.contains(enrollment.getEnrollmentCode() ))
										{ 
										HomTotal.add(enrollment.getStudentOid());
										}
									}
									else if (enrollment.getEnrollmentType().equals(CON_WITHDRAW) && !excludeWD.contains(enrollment.getEnrollmentCode() ))
									{ 
										HomTotal.add(enrollment.getStudentOid());
									}
								 }
								 }
							}				
						}
						}
						
						  
						 
						if (School.getFieldA010() != null && !School.getFieldA010().isEmpty())
						{
							int Field10 = Integer.parseInt(  School.getFieldA010());
							BigDecimal Field10B  =  new BigDecimal(Field10);
							OfficialEnrollment = OfficialEnrollment.add(Field10B);
						}	
						
			 }
						}
					 
					BigDecimal[] mRates = new BigDecimal[4];
					
					mRates[0]= new BigDecimal ( AllTotal.size()); 
					mRates[1]= new BigDecimal ( DisTotal.size()); 
					mRates[2]= new BigDecimal ( ELLTotal.size()); 
					mRates[3]= new BigDecimal ( HomTotal.size());
					  
						  mRates[0] = mRates[0].divide(OfficialEnrollment, 4,3);
						  mRates[1] = mRates[1].divide(OfficialEnrollment, 4,3);
						  mRates[2] = mRates[2].divide(OfficialEnrollment, 4,3);
						  mRates[3] = mRates[3].divide(OfficialEnrollment, 4,3);
						 
						  m_mobilityRates.put("1" , mRates); 
				 
					}
			 }
		
	  
	
		public class PairedEnrollment
		{
			public PlainDate RegistrationDate;
			public String studentOid;
			public PlainDate WithdrawDate;
			public String SchoolOid;
			public Integer Membership_days;
			public Integer Absences;
			public Integer Tardy;
			public Integer Unexcused;
			public Integer Excused;
			public Integer OSS;
			public Integer ISS;
	
	
			public PairedEnrollment( String SOid, String stu, PlainDate RegDate)
			{
				SchoolOid = SOid;
				studentOid = stu;
				RegistrationDate = RegDate;
				WithdrawDate = null;
				Membership_days = 0;
				Absences = 0;
				Tardy = 0;
				Unexcused = 0;
				Excused= 0;
				OSS = 0;
				ISS = 0;
			}
	
		}
		
	}