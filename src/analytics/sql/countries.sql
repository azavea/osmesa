CREATE TABLE countries (
    id SERIAL,
    name text,
    code text NOT NULL UNIQUE,
    PRIMARY KEY(id)
);

COPY countries (id, name, code) FROM stdin;
1	Alaska	USA-AK
2	Alabama	USA-AL
3	Arkansas	USA-AR
4	Arizona	USA-AZ
5	California	USA-CA
6	Colorado	USA-CO
7	Connecticut	USA-CT
8	District of Columbia	USA-DC
9	Delaware	USA-DE
10	Florida	USA-FL
11	Georgia	USA-GA
12	Hawaii	USA-HI
13	Iowa	USA-IA
14	Idaho	USA-ID
15	Illinois	USA-IL
16	Indiana	USA-IN
17	Kansas	USA-KS
18	Kentucky	USA-KY
19	Louisiana	USA-LA
20	Massachusetts	USA-MA
21	Maryland	USA-MD
22	Maine	USA-ME
23	Michigan	USA-MI
24	Minnesota	USA-MN
25	Missouri	USA-MO
26	Mississippi	USA-MS
27	Montana	USA-MT
28	North Carolina	USA-NC
29	North Dakota	USA-ND
30	Nebraska	USA-NE
31	New Hampshire	USA-NH
32	New Jersey	USA-NJ
33	New Mexico	USA-NM
34	Nevada	USA-NV
35	New York	USA-NY
36	Ohio	USA-OH
37	Oklahoma	USA-OK
38	Oregon	USA-OR
39	Pennsylvania	USA-PA
40	Puerto Rico	USA-PR
41	Rhode Island	USA-RI
42	South Carolina	USA-SC
43	South Dakota	USA-SD
44	Tennessee	USA-TN
45	Texas	USA-TX
46	Utah	USA-UT
47	Virginia	USA-VA
48	Vermont	USA-VT
49	Washington	USA-WA
50	Wisconsin	USA-WI
51	West Virginia	USA-WV
52	Wyoming	USA-WY
53	Afghanistan	AFG
54	Angola	AGO
55	Albania	ALB
56	United Arab Emirates	ARE
57	Argentina	ARG
58	Armenia	ARM
59	Antarctica	ATA
60	French Southern and Antarctic Lands	ATF
61	Australia	AUS
62	Austria	AUT
63	Azerbaijan	AZE
64	Burundi	BDI
65	Belgium	BEL
66	Benin	BEN
67	Burkina Faso	BFA
68	Bangladesh	BGD
69	Bulgaria	BGR
70	The Bahamas	BHS
71	Bosnia and Herzegovina	BIH
72	Belarus	BLR
73	Belize	BLZ
74	Bermuda	BMU
75	Bolivia	BOL
76	Brazil	BRA
77	Brunei	BRN
78	Bhutan	BTN
79	Botswana	BWA
80	Central African Republic	CAF
81	Canada	CAN
82	Switzerland	CHE
83	Chile	CHL
84	China	CHN
85	Ivory Coast	CIV
86	Cameroon	CMR
87	Democratic Republic of the Congo	COD
88	Republic of the Congo	COG
89	Colombia	COL
90	Costa Rica	CRI
91	Cuba	CUB
92	Northern Cyprus	CYN
93	Cyprus	CYP
94	Czech Republic	CZE
95	Germany	DEU
96	Djibouti	DJI
97	Denmark	DNK
98	Dominican Republic	DOM
99	Algeria	DZA
100	Ecuador	ECU
101	Egypt	EGY
102	Eritrea	ERI
103	Spain	ESP
104	Estonia	EST
105	Ethiopia	ETH
106	Finland	FIN
107	Fiji	FJI
108	Falkland Islands	FLK
109	France	FRA
110	Gabon	GAB
111	United Kingdom	GBR
112	Georgia	GEO
113	Ghana	GHA
114	Guinea	GIN
115	Gambia	GMB
116	Guinea Bissau	GNB
117	Equatorial Guinea	GNQ
118	Greece	GRC
119	Greenland	GRL
120	Guatemala	GTM
121	French Guiana	GUF
122	Guyana	GUY
123	Honduras	HND
124	Croatia	HRV
125	Haiti	HTI
126	Hungary	HUN
127	Indonesia	IDN
128	India	IND
129	Ireland	IRL
130	Iran	IRN
131	Iraq	IRQ
132	Iceland	ISL
133	Israel	ISR
134	Italy	ITA
135	Jamaica	JAM
136	Jordan	JOR
137	Japan	JPN
138	Kazakhstan	KAZ
139	Kenya	KEN
140	Kyrgyzstan	KGZ
141	Cambodia	KHM
142	South Korea	KOR
143	Kosovo	KOS
144	Kuwait	KWT
145	Laos	LAO
146	Lebanon	LBN
147	Liberia	LBR
148	Libya	LBY
149	Sri Lanka	LKA
150	Lesotho	LSO
151	Lithuania	LTU
152	Luxembourg	LUX
153	Latvia	LVA
154	Morocco	MAR
155	Moldova	MDA
156	Madagascar	MDG
157	Mexico	MEX
158	Macedonia	MKD
159	Mali	MLI
160	Myanmar	MMR
161	Montenegro	MNE
162	Mongolia	MNG
163	Mozambique	MOZ
164	Mauritania	MRT
165	Malawi	MWI
166	Singapore	SGP
167	Malaysia	MYS
168	Namibia	NAM
169	New Caledonia	NCL
170	Niger	NER
171	Nigeria	NGA
172	Nicaragua	NIC
173	Netherlands	NLD
174	Norway	NOR
175	Nepal	NPL
176	New Zealand	NZL
177	Oman	OMN
178	Pakistan	PAK
179	Panama	PAN
180	Peru	PER
181	Philippines	PHL
182	Papua New Guinea	PNG
183	Poland	POL
184	Puerto Rico	PRI
185	North Korea	PRK
186	Portugal	PRT
187	Paraguay	PRY
188	Qatar	QAT
189	Romania	ROU
190	Russia	RUS
191	Rwanda	RWA
192	Western Sahara	ESH
193	Saudi Arabia	SAU
194	Sudan	SDN
195	South Sudan	SDS
196	Senegal	SEN
197	Solomon Islands	SLB
198	Sierra Leone	SLE
199	El Salvador	SLV
201	Somalia	SOM
202	Republic of Serbia	SRB
203	Suriname	SUR
204	Slovakia	SVK
205	Slovenia	SVN
206	Sweden	SWE
207	Swaziland	SWZ
208	Syria	SYR
209	Chad	TCD
210	Togo	TGO
211	Thailand	THA
212	Tajikistan	TJK
213	Turkmenistan	TKM
214	East Timor	TLS
215	Trinidad and Tobago	TTO
216	Tunisia	TUN
217	Turkey	TUR
218	Taiwan	TWN
219	United Republic of Tanzania	TZA
220	Uganda	UGA
221	Ukraine	UKR
222	Uruguay	URY
223	United States of America	USA
224	Uzbekistan	UZB
225	Venezuela	VEN
226	Vietnam	VNM
227	Vanuatu	VUT
228	West Bank	PSE
229	Yemen	YEM
230	South Africa	ZAF
231	Zambia	ZMB
232	Zimbabwe	ZWE
233	Aruba	ABW
234	Anguilla	AIA
235	Åland Islands	ALD
236	Andorra	AND
237	American Samoa	ASM
238	Ashmore and Cartier Islands	ATC
239	Antigua and Barbuda	ATG
240	Bahrain	BHR
241	Bajo Nuevo Bank (Petrel Islands)	BJN
242	Saint-Barthélemy	BLM
243	Barbados	BRB
244	Clipperton Island	CLP
245	Cyprus U.N. Buffer Zone	CNM
246	Cook Islands	COK
247	Comoros	COM
248	Republic of Cabo Verde	CPV
249	Coral Sea Islands	CSI
250	Curaçao	CUW
251	Cayman Islands	CYM
252	Dominica	DMA
253	Dhekelia	ESB
254	Faeroe Islands	FRO
255	Federated States of Micronesia	FSM
256	Guernsey	GGY
257	Gibraltar	GIB
258	Grenada	GRD
259	Guam	GUM
260	Hong Kong	HKG
261	Heard Island and McDonald Islands	HMD
262	Isle of Man	IMN
263	Indian Ocean Territories	IOA
264	British Indian Ocean Territory	IOT
265	Jersey	JEY
266	Baikonur Cosmodrome	KAB
267	Siachen Glacier	KAS
268	Kiribati	KIR
269	Saint Kitts and Nevis	KNA
270	Saint Lucia	LCA
271	Liechtenstein	LIE
272	Macao	MAC
273	Saint-Martin	MAF
274	Monaco	MCO
275	Maldives	MDV
276	Marshall Islands	MHL
277	Malta	MLT
278	Northern Mariana Islands	MNP
279	Montserrat	MSR
280	Mauritius	MUS
281	Norfolk Island	NFK
282	Niue	NIU
283	Nauru	NRU
284	Pitcairn Islands	PCN
285	Spratly Islands	PGA
286	Palau	PLW
287	Palestine	PSX
288	French Polynesia	PYF
289	Western Sahara	SAH
290	Scarborough Reef	SCR
291	Serranilla Bank	SER
292	South Georgia and the Islands	SGS
293	Saint Helena	SHN
294	San Marino	SMR
295	Somaliland	SOL
296	Saint Pierre and Miquelon	SPM
297	São Tomé and Principe	STP
298	Sint Maarten	SXM
299	Seychelles	SYC
300	Turks and Caicos Islands	TCA
301	Tonga	TON
302	Tuvalu	TUV
303	United States Minor Outlying Islands	UMI
304	US Naval Base Guantanamo Bay	USG
305	Vatican	VAT
306	Saint Vincent and the Grenadines	VCT
307	British Virgin Islands	VGB
308	United States Virgin Islands	VIR
309	Wallis and Futuna Islands	WLF
310	Akrotiri	WSB
311	Samoa	WSM
\.
