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
import com.follett.fsc.core.k12.beans.Address;
import com.follett.fsc.core.k12.beans.GridCode;
import com.follett.fsc.core.k12.beans.Person;
import com.follett.fsc.core.k12.beans.ReferenceCode;
import com.follett.fsc.core.k12.beans.ReferenceTable;
import com.follett.fsc.core.k12.beans.SchoolGridCode;
import com.follett.fsc.core.k12.beans.X2BaseBean;
import com.follett.fsc.core.k12.business.X2Broker;
import com.follett.fsc.core.k12.tools.procedures.ProcedureJavaSource;
import com.follett.fsc.core.k12.web.UserDataContainer;
import com.x2dev.sis.model.beans.SisPerson;
import com.x2dev.sis.model.beans.SisSchool;
import com.x2dev.sis.model.beans.SisStudent;
import static com.follett.fsc.core.k12.business.ModelProperty.PATH_DELIMITER;

public class NextSchoolSetup  extends ProcedureJavaSource{

	private static final String 	GRADE_LEVELS 		=		"Grade Levels";
	private final String[] AVE	={ "AVE","AV","AVENUE"};
	private final String[] BLVD	={"BLVD","BL"};
	private final String[] CIR	={"CIR","CIRCLE"};
	private final String[] CT	={"CT","COURT"};
	private final String[] DR	={"DR","DRIVE","DRV"};
	private final String[] HWY	={ "HIGHWAY", "HWY"};
	private final String[] LANE = {"LANE","LN"};
	private final String[] PKWY = {"PKWY","PW"};
	private final String[]	PL ={"PL","PLACE"};
	private final String[] ROAD= {"ROAD","RD"};
	private final String[] ST ={"ST","STREET"};
	private final String TPK= "TPK";
	private final String WAY = "WAY";
	private final String[] ALL_TYPES = { "AVE","AV","AVENUE","BLVD","HIGHWAY","BL","CIR","CIRCLE","CT","COURT", "DR","DRIVE","DRV", "HWY","LANE","LN",
										"PKWY","PW","PL","PLACE", "ROAD","RD", "ST","STREET", "TPK","WAY"};
	private List<String> L_ave;
	private List<String> L_blvd;
	private List<String> L_cir;
	private List<String> L_ct;
	private List<String> L_dr;
	private List<String> L_lane;
	private List<String> L_pl;
	private List<String> L_pkwy;
	private List<String> L_road;
	private List<String> L_st;
	private List<String> L_hwy;
	private List<String> L_all;
	
	

	private Collection<SisStudent> m_students;  // Collection of Students
	private Map<String, Person> m_persons;	// Collection of Addresses for the students above
	private Map<String, Address> m_addresses;	// Collection of Addresses for the students above
	private Map<String, SisSchool> m_schools;	// ALL active Schools mapped by school OID 
	private Map<String, Collection <GridCode>> m_gridCodes;	// ALL Grid codes mapped by Postal code then Street name
	private Map<String, Collection<SchoolGridCode>> m_gridSchools; //Map of Grid OID and Collection of School OIDs that belongs to that grid.
	private Map<String, ReferenceCode>	m_gradeMap; // Map of grade levels and the int values.
	private Map<String, Integer[]> m_schoolGrades; // Starting grade level and Last grade level for each schools.  

	private SisSchool m_currentSchool;

	@Override
	protected void execute() throws Exception {
		// TODO Auto-generated method stub
		L_ave      = new ArrayList<String>(Arrays.asList(AVE));
		L_blvd      = new ArrayList<String>(Arrays.asList(BLVD));
		L_cir      = new ArrayList<String>(Arrays.asList(CIR));
		L_ct      = new ArrayList<String>(Arrays.asList(CT));
		L_dr      = new ArrayList<String>(Arrays.asList(DR));
		L_lane      = new ArrayList<String>(Arrays.asList(LANE));
		L_pl      = new ArrayList<String>(Arrays.asList(PL));
		L_road      = new ArrayList<String>(Arrays.asList(ROAD));
		L_st      = new ArrayList<String>(Arrays.asList(ST));
		L_pkwy      = new ArrayList<String>(Arrays.asList(PKWY));
		L_hwy      = new ArrayList<String>(Arrays.asList(HWY));
		L_all     = new ArrayList<String>(Arrays.asList(ALL_TYPES));
		LoadStudents();
		LoadSchools();
		LoadGradeMap();
		LoadGrids();

		ProcessStudents();
		 

	}

	private void ProcessStudents()
	{
		
		int StudentCount =0;
		int ErrorCount =0;
		int ProcessedCount =0;
		int SameSchool =0;

		for (SisStudent student: m_students)
		{
			StudentCount++;
			SisSchool currentSchool = m_schools.get(student.getSchoolOid());
			String gradeString = null;
			if (m_gradeMap.containsKey(student.getGradeLevel()))  gradeString = m_gradeMap.get(student.getGradeLevel()).getFieldA005();
			if (gradeString != null)
			{
				int StudentGrade = Integer.parseInt(gradeString);
				StudentGrade ++;
				Integer[] gradeRange = m_schoolGrades.get(currentSchool.getOid());  

				if (StudentGrade <= gradeRange[1] || gradeString.equals("12"))  // if Current school covers the student's next year grade
				{
					SameSchool++;
					//logMessage("Student " + student.getNameView() + " ID: " + student.getLocalId()+ "  assigned to same school" );
					student.setNextSchoolOid(currentSchool.getOid());
					getBroker().saveBean(student);
				}
				else  // if Next school needs to be found.
				{
					Person per ;  
					if 	(student.getPersonOid() != null && m_persons.containsKey(student.getPersonOid()))
					{
						per = m_persons.get(student.getPersonOid());

						if (per.getPhysicalAddressOid() != null && m_addresses.containsKey(per.getPhysicalAddressOid()))
						{
							Address add = m_addresses.get(per.getPhysicalAddressOid());

							String PC = add.getPostalCode();
							String StreetName = add.getStreetName() ; 
							String StreetType = add.getStreetType();
							String StreetDir = add.getStreetPreDirection();
							 
							if (StreetName!=null )
							{
							String[] temp = DecodeStreetName(StreetName, StreetType);	
							StreetName = temp[0];
							StreetType = temp[1];
							 
							if (StreetDir != null)
							{
								StreetDir = StreetDir.toUpperCase();
								StreetDir = StreetDir.replaceAll("[.]", "");
							}
							
							int StreetNum = add.getStreetNumber(); 
							
							if (m_gridCodes.containsKey(StreetName))
							{
								 
									Collection <GridCode> Code = 	m_gridCodes.get(StreetName) ; 
									String GridCodeOid = null;
									for (GridCode code:Code)
									{
										String StreetType2 = code.getStreetType(); 
										
										if (StreetType2 != null) StreetType2 = StreetType2.toUpperCase();
										if (StreetType  != null) StreetType = StreetType.toUpperCase();
										
										if( StreetType== null||(  StreetType != null && StreetType2!= null && StreetType2.equals(StreetType)))
										{
										if(StreetDir == null || (StreetDir != null && code.getStreetPreDirection() != null && code.getStreetPreDirection().equals(StreetDir)))
										{
											int EvenOdd =code.getStreetSide() ; 
											
											int StreetEO =0;
											
											if (StreetNum%2 ==0) StreetEO = 2; else StreetEO = 1; 
												
											if ( StreetNum >= code.getFirstStreetNumber() && StreetNum <= code.getLastStreetNumber() && StreetEO == EvenOdd)
											{
									 	//	logMessage("Student " + student.getNameView() + " ID: " + student.getLocalId()+ " found coordinate : " + code.getGridCode() +" "  + code.getStreetView() +" "+ EvenOdd +"  Street Even/Odd "+ StreetEO);
												GridCodeOid = code.getOid();
												break;
											}

										}
										}
									}

									if (GridCodeOid == null) 
									{
										logMessage("Student " + student.getNameView() + "	ID: " + student.getLocalId()+ "	No Valid Gridcode Found. "   );
										ErrorCount++;
									}
									
									if ( GridCodeOid !=null)
									{
									if(	AssignSchool(student,StudentGrade, GridCodeOid)) ProcessedCount++;
									else 
										{ErrorCount++;
										logMessage("Student " + student.getNameView() + "	ID: " + student.getLocalId()+ "	Unable to find valid school.");
										}
									}
								 
							}
							else
							{
								logMessage("Student " + student.getNameView() + "	ID: " + student.getLocalId()+ "	:Street Name not found '" +StreetName +"' " );
								ErrorCount++;
							}
							}
							else
							{
								logMessage("Student " + student.getNameView() + "	ID: " + student.getLocalId()+ "	: Invalid Address ");
								ErrorCount++;
							}
 
						}// has address
						else
						{
							logMessage("Student " + student.getNameView() + "	ID: " + student.getLocalId()+ "	No Physical Address Foun");
							ErrorCount++;

						} //has person
					}
					else
					{
						logMessage("Student " + student.getNameView() + "	ID: " + student.getLocalId()+ "	Does not have Person");
						ErrorCount++;
					}
				}  ///find next school
			}  /// gradeString is not null
		}/// Loop per student
		logMessage( "------------------------------------------------------------------------");
		logMessage(" ");
		logMessage( "Students Processed                     : " +StudentCount);
		logMessage( "Students Assigned To Another School    : " +ProcessedCount);
		logMessage( "Students Reassigned To current School  : " +SameSchool); 
		logMessage( "Students Not Assigned                  : " +ErrorCount);
		
	}
	
	
	private boolean AssignSchool(final SisStudent  student,int studentGrade, String Oid)
	{
		
		if (m_gridSchools.containsKey(Oid))
		{
			Collection<SchoolGridCode> schoolGrids = m_gridSchools.get(Oid);
			
			for (SchoolGridCode  school:schoolGrids )
			{
				String SchoolOid = school.getSchoolOid();
				Integer[] grades = m_schoolGrades.get(SchoolOid);
				
				if (grades[0].intValue() == studentGrade)
				{ 
					student.setNextSchoolOid(SchoolOid);
					getBroker().saveBean(student);
					return true;
				}
			}
		}
		return false;
		
	}

	private void LoadStudents()
	{
		Criteria m_studentCriteria = getCurrentCriteria(); 

		SubQuery stuAddSub =  new SubQuery(SisStudent.class, SisStudent.REL_PERSON +  PATH_DELIMITER + SisPerson.COL_PHYSICAL_ADDRESS_OID , m_studentCriteria); 
		SubQuery stuPerSub =  new SubQuery(SisStudent.class, SisStudent.COL_PERSON_OID , m_studentCriteria);

		QueryByCriteria query = new QueryByCriteria(SisStudent.class, m_studentCriteria);
		query.addOrderByAscending(SisStudent.COL_NAME_VIEW);
		m_students = getBroker().getCollectionByQuery(query);

		// Person
		Criteria perCriteria = new Criteria();
		perCriteria.addIn(Person.COL_OID, stuPerSub);
		QueryByCriteria perQuery = new QueryByCriteria(Person.class, perCriteria);
		m_persons = getBroker().getMapByQuery(perQuery,Person.COL_OID , 20000);


		// Address
		Criteria addCriteria = new Criteria();
		addCriteria.addIn(Address.COL_OID, stuAddSub);
		QueryByCriteria AddQuery = new QueryByCriteria(Address.class, addCriteria);
		m_addresses = getBroker().getMapByQuery(AddQuery,Address.COL_OID , 20000);

	}

	private void LoadSchools()
	{
		Criteria schoolCriteria = new Criteria();
		schoolCriteria.addEqualTo(SisSchool.COL_INACTIVE_INDICATOR, false);
		QueryByCriteria query = new QueryByCriteria(SisSchool.class,schoolCriteria );
		SubQuery schoolSub = new SubQuery(SisSchool.class, SisSchool.COL_ADDRESS_OID, schoolCriteria);
		m_schools = getBroker().getMapByQuery(query, SisSchool.COL_OID, 100); 
		m_schoolGrades = new HashMap<String, Integer[]>();

		for (SisSchool school: m_schools.values())
		{
			Integer StartGrade;
			Integer EndGrade;
			int size;
			StartGrade = new Integer(school.getStartGrade());
			size = school.getNumberOfGrades();
			EndGrade = new Integer(StartGrade.intValue() + size -1);

			m_schoolGrades.put(school.getOid(), new Integer[]{StartGrade, EndGrade} );
		}
	}

	private void LoadGradeMap()
	{
		Criteria criteria = new Criteria();
		criteria.addEqualTo(ReferenceCode.REL_REFERENCE_TABLE + PATH_DELIMITER + ReferenceTable.COL_USER_NAME, GRADE_LEVELS );
		QueryByCriteria query = new  QueryByCriteria(ReferenceCode.class, criteria);
		m_gradeMap = getBroker().getMapByQuery(query, ReferenceCode.COL_CODE,20);

	}

	private void LoadGrids()
	{
		Criteria criteria = new Criteria();
		QueryByCriteria query = new QueryByCriteria(GridCode.class, criteria);
		query.addOrderByAscending(GridCode.COL_STREET_NAME);
		query.addOrderByAscending(GridCode.COL_STREET_TYPE);
		Collection<GridCode> gridcodes = getBroker().getCollectionByQuery( query  );

		Criteria schoolCriteria = new Criteria();
		QueryByCriteria schoolQuery = new QueryByCriteria(SchoolGridCode.class, schoolCriteria);
		m_gridSchools = getBroker().getGroupedCollectionByQuery(schoolQuery, SchoolGridCode.COL_GRID_CODE_OID, 5000);

		
		m_gridCodes = new HashMap<String, Collection<GridCode>>();
		Collection<GridCode> Grids = new HashSet<GridCode>();
		String LastName = null;
		for (GridCode Grid :gridcodes)
		{
			if (LastName == null)
			{
				LastName = Grid.getStreetName();
			}
			
			if (!LastName.equals(Grid.getStreetName()))
			{
				m_gridCodes.put(LastName.toUpperCase(), Grids); 
				Grids = new HashSet<GridCode>();
			}
			Grids.add(Grid);
			LastName = Grid.getStreetName();
		}
		
		if (!Grids.isEmpty()) m_gridCodes.put(LastName.toUpperCase(), Grids);
			

	}

	private String[] DecodeStreetName(String StreetName, String StreetType)
	{
		String[]   Street = new String[2];
		Street[1]=""; 
		String TempString = StreetName; 
		TempString = TempString.replaceAll("[.]", "");
		TempString = TempString.toUpperCase();
		int LastSpace = TempString.lastIndexOf(" "); 
		String temp= StreetType;
		if ( temp == null)  temp = "";
		
		if (LastSpace ==-1)
		{
		Street[0] = TempString;  
		}
		else
		{  
		temp = TempString.substring(LastSpace +1);
		temp =temp.toUpperCase();
		if (L_all.contains(temp))
		{ 
		Street[0] = TempString.substring(0, LastSpace ); 
		}
		else
		{  
		Street[0] = TempString ;
		temp= StreetType;
		if ( temp == null)  temp = "";
		}
		
		}
		temp= temp.toUpperCase();
		
		if (L_ave.contains(temp))			Street[1] = "AVE";
		if (L_blvd.contains(temp)) 			Street[1] = "BLVD";
		if (L_cir.contains(temp)) 			Street[1] = "CIR";
		if (L_ct.contains(temp))			Street[1] = "CT";
		if (L_dr.contains(temp)) 			Street[1] = "DR";
		if (L_lane.contains(temp)) 			Street[1] = "LANE";
		if (L_pl.contains(temp)) 			Street[1] = "PL";
		if (L_road.contains(temp)) 			Street[1] = "ROAD";
		if (L_st.contains(temp)) 			Street[1] = "ST";
		if (L_pkwy.contains(temp)) 			Street[1] = "PKWY";
		if (L_hwy.contains(temp)) 			Street[1] = "HWY";
		if ( temp.equals(TPK) ||temp.equals(WAY)) 	Street[1] =temp;
		 
		
		return Street;
		
	}

}