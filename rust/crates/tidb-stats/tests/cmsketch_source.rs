// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Direct source-owned tests for the dependency-closed CMSketch boundary.
//!
//! These tests intentionally exercise encoded bytes and caller-owned hashes.
//! Datum/tablecodec encoding, sampled TopN construction, protobuf snapshots,
//! and the statistics handle are not silently replaced by test fixtures.

use std::collections::HashMap;

use tidb_datatype::Datum;
use tidb_stats::cmsketch::encode_integer_datum_value;
use tidb_stats::{
    decode_cmsketch, decode_cmsketch_and_embedded_topn, decode_cmsketch_and_topn,
    encode_cmsketch_and_topn, encode_cmsketch_without_topn, get_merged_topn_from_sorted_slice,
    hash_bytes, merge_topn, merge_topn_and_update_cmsketch, new_cmsketch_and_topn,
    new_cmsketch_and_topn_with_tie_stabilization, sort_topn_meta, topn_meta_compare, CmsSketch,
    Hash128, MergeError, TopN, TopNEntry,
};

fn build_seeded_zipf_sketch(
    seed: i64,
    total: u64,
    imax: u64,
    factor: f64,
) -> (CmsSketch, HashMap<i64, u32>) {
    let mut zipf = GoZipf::new(seed, factor, 1.0, imax);
    let mut sketch = CmsSketch::new(5, 2_048);
    let mut counts = HashMap::new();
    for _ in 0..total {
        let value = zipf.next() as i64;
        let datum = Datum::new_int(value);
        let encoded = encode_integer_datum_value(&datum).expect("integer EncodeValue");
        sketch.insert_bytes(&encoded);
        *counts.entry(value).or_insert(0) += 1;
    }
    (sketch, counts)
}

fn average_absolute_integer_error(
    sketch: &CmsSketch,
    topn: Option<&TopN>,
    counts: &HashMap<i64, u32>,
) -> u64 {
    let total = counts.iter().fold(0_u64, |total, (&value, &count)| {
        let estimate = sketch
            .query_integer_datum(topn, &Datum::new_int(value))
            .expect("integer QueryValue");
        total.wrapping_add(u64::from(count).abs_diff(estimate))
    });
    total / counts.len() as u64
}

const GO_RNG_LEN: usize = 607;
const GO_RNG_TAP: usize = 273;
const GO_RNG_MASK: u64 = (1_u64 << 63) - 1;
const GO_RNG_COOKED: [i64; GO_RNG_LEN] = [
    -4181792142133755926,
    -4576982950128230565,
    1395769623340756751,
    5333664234075297259,
    -6347679516498800754,
    9033628115061424579,
    7143218595135194537,
    4812947590706362721,
    7937252194349799378,
    5307299880338848416,
    8209348851763925077,
    -7107630437535961764,
    4593015457530856296,
    8140875735541888011,
    -5903942795589686782,
    -603556388664454774,
    -7496297993371156308,
    113108499721038619,
    4569519971459345583,
    -4160538177779461077,
    -6835753265595711384,
    -6507240692498089696,
    6559392774825876886,
    7650093201692370310,
    7684323884043752161,
    -8965504200858744418,
    -2629915517445760644,
    271327514973697897,
    -6433985589514657524,
    1065192797246149621,
    3344507881999356393,
    -4763574095074709175,
    7465081662728599889,
    1014950805555097187,
    -4773931307508785033,
    -5742262670416273165,
    2418672789110888383,
    5796562887576294778,
    4484266064449540171,
    3738982361971787048,
    -4699774852342421385,
    10530508058128498,
    -589538253572429690,
    -6598062107225984180,
    8660405965245884302,
    10162832508971942,
    -2682657355892958417,
    7031802312784620857,
    6240911277345944669,
    831864355460801054,
    -1218937899312622917,
    2116287251661052151,
    2202309800992166967,
    9161020366945053561,
    4069299552407763864,
    4936383537992622449,
    457351505131524928,
    -8881176990926596454,
    -6375600354038175299,
    -7155351920868399290,
    4368649989588021065,
    887231587095185257,
    -3659780529968199312,
    -2407146836602825512,
    5616972787034086048,
    -751562733459939242,
    1686575021641186857,
    -5177887698780513806,
    -4979215821652996885,
    -1375154703071198421,
    5632136521049761902,
    -8390088894796940536,
    -193645528485698615,
    -5979788902190688516,
    -4907000935050298721,
    -285522056888777828,
    -2776431630044341707,
    1679342092332374735,
    6050638460742422078,
    -2229851317345194226,
    -1582494184340482199,
    5881353426285907985,
    812786550756860885,
    4541845584483343330,
    -6497901820577766722,
    4980675660146853729,
    -4012602956251539747,
    -329088717864244987,
    -2896929232104691526,
    1495812843684243920,
    -2153620458055647789,
    7370257291860230865,
    -2466442761497833547,
    4706794511633873654,
    -1398851569026877145,
    8549875090542453214,
    -9189721207376179652,
    -7894453601103453165,
    7297902601803624459,
    1011190183918857495,
    -6985347000036920864,
    5147159997473910359,
    -8326859945294252826,
    2659470849286379941,
    6097729358393448602,
    -7491646050550022124,
    -5117116194870963097,
    -896216826133240300,
    -745860416168701406,
    5803876044675762232,
    -787954255994554146,
    -3234519180203704564,
    -4507534739750823898,
    -1657200065590290694,
    505808562678895611,
    -4153273856159712438,
    -8381261370078904295,
    572156825025677802,
    1791881013492340891,
    3393267094866038768,
    -5444650186382539299,
    2352769483186201278,
    -7930912453007408350,
    -325464993179687389,
    -3441562999710612272,
    -6489413242825283295,
    5092019688680754699,
    -227247482082248967,
    4234737173186232084,
    5027558287275472836,
    4635198586344772304,
    -536033143587636457,
    5907508150730407386,
    -8438615781380831356,
    972392927514829904,
    -3801314342046600696,
    -4064951393885491917,
    -174840358296132583,
    2407211146698877100,
    -1640089820333676239,
    3940796514530962282,
    -5882197405809569433,
    3095313889586102949,
    -1818050141166537098,
    5832080132947175283,
    7890064875145919662,
    8184139210799583195,
    -8073512175445549678,
    -7758774793014564506,
    -4581724029666783935,
    3516491885471466898,
    -8267083515063118116,
    6657089965014657519,
    5220884358887979358,
    1796677326474620641,
    5340761970648932916,
    1147977171614181568,
    5066037465548252321,
    2574765911837859848,
    1085848279845204775,
    -5873264506986385449,
    6116438694366558490,
    2107701075971293812,
    -7420077970933506541,
    2469478054175558874,
    -1855128755834809824,
    -5431463669011098282,
    -9038325065738319171,
    -6966276280341336160,
    7217693971077460129,
    -8314322083775271549,
    7196649268545224266,
    -3585711691453906209,
    -5267827091426810625,
    8057528650917418961,
    -5084103596553648165,
    -2601445448341207749,
    -7850010900052094367,
    6527366231383600011,
    3507654575162700890,
    9202058512774729859,
    1954818376891585542,
    -2582991129724600103,
    8299563319178235687,
    -5321504681635821435,
    7046310742295574065,
    -2376176645520785576,
    -7650733936335907755,
    8850422670118399721,
    3631909142291992901,
    5158881091950831288,
    -6340413719511654215,
    4763258931815816403,
    6280052734341785344,
    -4979582628649810958,
    2043464728020827976,
    -2678071570832690343,
    4562580375758598164,
    5495451168795427352,
    -7485059175264624713,
    553004618757816492,
    6895160632757959823,
    -989748114590090637,
    7139506338801360852,
    -672480814466784139,
    5535668688139305547,
    2430933853350256242,
    -3821430778991574732,
    -1063731997747047009,
    -3065878205254005442,
    7632066283658143750,
    6308328381617103346,
    3681878764086140361,
    3289686137190109749,
    6587997200611086848,
    244714774258135476,
    -5143583659437639708,
    8090302575944624335,
    2945117363431356361,
    -8359047641006034763,
    3009039260312620700,
    -793344576772241777,
    401084700045993341,
    -1968749590416080887,
    4707864159563588614,
    -3583123505891281857,
    -3240864324164777915,
    -5908273794572565703,
    -3719524458082857382,
    -5281400669679581926,
    8118566580304798074,
    3839261274019871296,
    7062410411742090847,
    -8481991033874568140,
    6027994129690250817,
    -6725542042704711878,
    -2971981702428546974,
    -7854441788951256975,
    8809096399316380241,
    6492004350391900708,
    2462145737463489636,
    -8818543617934476634,
    -5070345602623085213,
    -8961586321599299868,
    -3758656652254704451,
    -8630661632476012791,
    6764129236657751224,
    -709716318315418359,
    -3403028373052861600,
    -8838073512170985897,
    -3999237033416576341,
    -2920240395515973663,
    -2073249475545404416,
    368107899140673753,
    -6108185202296464250,
    -6307735683270494757,
    4782583894627718279,
    6718292300699989587,
    8387085186914375220,
    3387513132024756289,
    4654329375432538231,
    -292704475491394206,
    -3848998599978456535,
    7623042350483453954,
    7725442901813263321,
    9186225467561587250,
    -5132344747257272453,
    -6865740430362196008,
    2530936820058611833,
    1636551876240043639,
    -3658707362519810009,
    1452244145334316253,
    -7161729655835084979,
    -7943791770359481772,
    9108481583171221009,
    -3200093350120725999,
    5007630032676973346,
    2153168792952589781,
    6720334534964750538,
    -3181825545719981703,
    3433922409283786309,
    2285479922797300912,
    3110614940896576130,
    -2856812446131932915,
    -3804580617188639299,
    7163298419643543757,
    4891138053923696990,
    580618510277907015,
    1684034065251686769,
    4429514767357295841,
    -8893025458299325803,
    -8103734041042601133,
    7177515271653460134,
    4589042248470800257,
    -1530083407795771245,
    143607045258444228,
    246994305896273627,
    -8356954712051676521,
    6473547110565816071,
    3092379936208876896,
    2058427839513754051,
    -4089587328327907870,
    8785882556301281247,
    -3074039370013608197,
    -637529855400303673,
    6137678347805511274,
    -7152924852417805802,
    5708223427705576541,
    -3223714144396531304,
    4358391411789012426,
    325123008708389849,
    6837621693887290924,
    4843721905315627004,
    -3212720814705499393,
    -3825019837890901156,
    4602025990114250980,
    1044646352569048800,
    9106614159853161675,
    -8394115921626182539,
    -4304087667751778808,
    2681532557646850893,
    3681559472488511871,
    -3915372517896561773,
    -2889241648411946534,
    -6564663803938238204,
    -8060058171802589521,
    581945337509520675,
    3648778920718647903,
    -4799698790548231394,
    -7602572252857820065,
    220828013409515943,
    -1072987336855386047,
    4287360518296753003,
    -4633371852008891965,
    5513660857261085186,
    -2258542936462001533,
    -8744380348503999773,
    8746140185685648781,
    228500091334420247,
    1356187007457302238,
    3019253992034194581,
    3152601605678500003,
    -8793219284148773595,
    5559581553696971176,
    4916432985369275664,
    -8559797105120221417,
    -5802598197927043732,
    2868348622579915573,
    -7224052902810357288,
    -5894682518218493085,
    2587672709781371173,
    -7706116723325376475,
    3092343956317362483,
    -5561119517847711700,
    972445599196498113,
    -1558506600978816441,
    1708913533482282562,
    -2305554874185907314,
    -6005743014309462908,
    -6653329009633068701,
    -483583197311151195,
    2488075924621352812,
    -4529369641467339140,
    -4663743555056261452,
    2997203966153298104,
    1282559373026354493,
    240113143146674385,
    8665713329246516443,
    628141331766346752,
    -4651421219668005332,
    -7750560848702540400,
    7596648026010355826,
    -3132152619100351065,
    7834161864828164065,
    7103445518877254909,
    4390861237357459201,
    -4780718172614204074,
    -319889632007444440,
    622261699494173647,
    -3186110786557562560,
    -8718967088789066690,
    -1948156510637662747,
    -8212195255998774408,
    -7028621931231314745,
    2623071828615234808,
    -4066058308780939700,
    -5484966924888173764,
    -6683604512778046238,
    -6756087640505506466,
    5256026990536851868,
    7841086888628396109,
    6640857538655893162,
    -8021284697816458310,
    -7109857044414059830,
    -1689021141511844405,
    -4298087301956291063,
    -4077748265377282003,
    -998231156719803476,
    2719520354384050532,
    9132346697815513771,
    4332154495710163773,
    -2085582442760428892,
    6994721091344268833,
    -2556143461985726874,
    -8567931991128098309,
    59934747298466858,
    -3098398008776739403,
    -265597256199410390,
    2332206071942466437,
    -7522315324568406181,
    3154897383618636503,
    -7585605855467168281,
    -6762850759087199275,
    197309393502684135,
    -8579694182469508493,
    2543179307861934850,
    4350769010207485119,
    -4468719947444108136,
    -7207776534213261296,
    -1224312577878317200,
    4287946071480840813,
    8362686366770308971,
    6486469209321732151,
    -5605644191012979782,
    -1669018511020473564,
    4450022655153542367,
    -7618176296641240059,
    -3896357471549267421,
    -4596796223304447488,
    -6531150016257070659,
    -8982326463137525940,
    -4125325062227681798,
    -1306489741394045544,
    -8338554946557245229,
    5329160409530630596,
    7790979528857726136,
    4955070238059373407,
    -4304834761432101506,
    -6215295852904371179,
    3007769226071157901,
    -6753025801236972788,
    8928702772696731736,
    7856187920214445904,
    -4748497451462800923,
    7900176660600710914,
    -7082800908938549136,
    -6797926979589575837,
    -6737316883512927978,
    4186670094382025798,
    1883939007446035042,
    -414705992779907823,
    3734134241178479257,
    4065968871360089196,
    6953124200385847784,
    -7917685222115876751,
    -7585632937840318161,
    -5567246375906782599,
    -5256612402221608788,
    3106378204088556331,
    -2894472214076325998,
    4565385105440252958,
    1979884289539493806,
    -6891578849933910383,
    3783206694208922581,
    8464961209802336085,
    2843963751609577687,
    3030678195484896323,
    -4429654462759003204,
    4459239494808162889,
    402587895800087237,
    8057891408711167515,
    4541888170938985079,
    1042662272908816815,
    -3666068979732206850,
    2647678726283249984,
    2144477441549833761,
    -3417019821499388721,
    -2105601033380872185,
    5916597177708541638,
    -8760774321402454447,
    8833658097025758785,
    5970273481425315300,
    563813119381731307,
    -6455022486202078793,
    1598828206250873866,
    -4016978389451217698,
    -2988328551145513985,
    -6071154634840136312,
    8469693267274066490,
    125672920241807416,
    -3912292412830714870,
    -2559617104544284221,
    -486523741806024092,
    -4735332261862713930,
    5923302823487327109,
    -9082480245771672572,
    -1808429243461201518,
    7990420780896957397,
    4317817392807076702,
    3625184369705367340,
    -6482649271566653105,
    -3480272027152017464,
    -3225473396345736649,
    -368878695502291645,
    -3981164001421868007,
    -8522033136963788610,
    7609280429197514109,
    3020985755112334161,
    -2572049329799262942,
    2635195723621160615,
    5144520864246028816,
    -8188285521126945980,
    1567242097116389047,
    8172389260191636581,
    -2885551685425483535,
    -7060359469858316883,
    -6480181133964513127,
    -7317004403633452381,
    6011544915663598137,
    5932255307352610768,
    2241128460406315459,
    -8327867140638080220,
    3094483003111372717,
    4583857460292963101,
    9079887171656594975,
    -384082854924064405,
    -3460631649611717935,
    4225072055348026230,
    -7385151438465742745,
    3801620336801580414,
    -399845416774701952,
    -7446754431269675473,
    7899055018877642622,
    5421679761463003041,
    5521102963086275121,
    -4975092593295409910,
    8735487530905098534,
    -7462844945281082830,
    -2080886987197029914,
    -1000715163927557685,
    -4253840471931071485,
    -5828896094657903328,
    6424174453260338141,
    359248545074932887,
    -5949720754023045210,
    -2426265837057637212,
    3030918217665093212,
    -9077771202237461772,
    -3186796180789149575,
    740416251634527158,
    -2142944401404840226,
    6951781370868335478,
    399922722363687927,
    -8928469722407522623,
    -1378421100515597285,
    -8343051178220066766,
    -3030716356046100229,
    -8811767350470065420,
    9026808440365124461,
    6440783557497587732,
    4615674634722404292,
    539897290441580544,
    2096238225866883852,
    8751955639408182687,
    -7316147128802486205,
    7381039757301768559,
    6157238513393239656,
    -1473377804940618233,
    8629571604380892756,
    5280433031239081479,
    7101611890139813254,
    2479018537985767835,
    7169176924412769570,
    -1281305539061572506,
    -7865612307799218120,
    2278447439451174845,
    3625338785743880657,
    6477479539006708521,
    8976185375579272206,
    -3712000482142939688,
    1326024180520890843,
    7537449876596048829,
    5464680203499696154,
    3189671183162196045,
    6346751753565857109,
    -8982212049534145501,
    -6127578587196093755,
    -245039190118465649,
    -6320577374581628592,
    7208698530190629697,
    7276901792339343736,
    -7490986807540332668,
    4133292154170828382,
    2918308698224194548,
    -7703910638917631350,
    -3929437324238184044,
    -4300543082831323144,
    -6344160503358350167,
    5896236396443472108,
    -758328221503023383,
    -1894351639983151068,
    -307900319840287220,
    -6278469401177312761,
    -2171292963361310674,
    8382142935188824023,
    9103922860780351547,
    4152330101494654406,
];

struct GoRng {
    tap: usize,
    feed: usize,
    values: [i64; GO_RNG_LEN],
}

impl GoRng {
    fn new(mut seed: i64) -> Self {
        const INT32_MAX: i64 = (1_i64 << 31) - 1;
        seed %= INT32_MAX;
        if seed < 0 {
            seed += INT32_MAX;
        }
        if seed == 0 {
            seed = 89_482_311;
        }

        let mut values = [0_i64; GO_RNG_LEN];
        let mut x = seed as i32;
        for index in -20_i32..GO_RNG_LEN as i32 {
            x = go_seed_rand(x);
            if index >= 0 {
                let mut value = i64::from(x).wrapping_shl(40);
                x = go_seed_rand(x);
                value ^= i64::from(x).wrapping_shl(20);
                x = go_seed_rand(x);
                value ^= i64::from(x);
                value ^= GO_RNG_COOKED[index as usize];
                values[index as usize] = value;
            }
        }
        Self {
            tap: 0,
            feed: GO_RNG_LEN - GO_RNG_TAP,
            values,
        }
    }

    fn int63(&mut self) -> i64 {
        self.tap = self.tap.checked_sub(1).unwrap_or(GO_RNG_LEN - 1);
        self.feed = self.feed.checked_sub(1).unwrap_or(GO_RNG_LEN - 1);
        let value = self.values[self.feed].wrapping_add(self.values[self.tap]);
        self.values[self.feed] = value;
        (value as u64 & GO_RNG_MASK) as i64
    }

    fn float64(&mut self) -> f64 {
        loop {
            let value = self.int63() as f64 / (1_u64 << 63) as f64;
            if value != 1.0 {
                return value;
            }
        }
    }
}

fn go_seed_rand(value: i32) -> i32 {
    const A: i32 = 48_271;
    const Q: i32 = 44_488;
    const R: i32 = 3_399;
    const INT32_MAX: i32 = i32::MAX;
    let high = value / Q;
    let low = value % Q;
    let next = A * low - R * high;
    if next < 0 {
        next + INT32_MAX
    } else {
        next
    }
}

struct GoZipf {
    random: GoRng,
    imax: f64,
    value: f64,
    q: f64,
    one_minus_q: f64,
    one_minus_q_inverse: f64,
    h_imax: f64,
    h_zero_minus_h_imax: f64,
    squeeze: f64,
}

impl GoZipf {
    fn new(seed: i64, q: f64, value: f64, imax: u64) -> Self {
        assert!(q > 1.0 && value >= 1.0);
        let one_minus_q = 1.0 - q;
        let one_minus_q_inverse = 1.0 / one_minus_q;
        let h = |x: f64| ((value + x).ln() * one_minus_q).exp() * one_minus_q_inverse;
        let h_inverse = |x: f64| ((one_minus_q * x).ln() * one_minus_q_inverse).exp() - value;
        let h_imax = h(imax as f64 + 0.5);
        let h_zero_minus_h_imax = h(0.5) - (-q * value.ln()).exp() - h_imax;
        let squeeze = 1.0 - h_inverse(h(1.5) - (-q * (value + 1.0).ln()).exp());
        Self {
            random: GoRng::new(seed),
            imax: imax as f64,
            value,
            q,
            one_minus_q,
            one_minus_q_inverse,
            h_imax,
            h_zero_minus_h_imax,
            squeeze,
        }
    }

    fn h(&self, x: f64) -> f64 {
        ((self.value + x).ln() * self.one_minus_q).exp() * self.one_minus_q_inverse
    }

    fn h_inverse(&self, x: f64) -> f64 {
        ((self.one_minus_q * x).ln() * self.one_minus_q_inverse).exp() - self.value
    }

    fn next(&mut self) -> u64 {
        loop {
            let uniform = self.random.float64();
            let transformed = self.h_imax + uniform * self.h_zero_minus_h_imax;
            let x = self.h_inverse(transformed);
            let candidate = (x + 0.5).floor();
            if candidate - x <= self.squeeze
                || transformed
                    >= self.h(candidate + 0.5) - (-(candidate + self.value).ln() * self.q).exp()
            {
                debug_assert!(candidate >= 0.0 && candidate <= self.imax);
                return candidate as u64;
            }
        }
    }
}

#[test]
fn murmur3_hash_vectors_match_go_sum128() {
    assert_eq!(hash_bytes(b""), Hash128 { h1: 0, h2: 0 });
    assert_eq!(
        hash_bytes(b"foo"),
        Hash128 {
            h1: 0xe271_8657_01f5_4561,
            h2: 0x7eaf_87e4_2bba_7d87,
        }
    );
    assert_eq!(
        hash_bytes(b"hello"),
        Hash128 {
            h1: 0xcbd8_a7b3_41bd_9b02,
            h2: 0x5b1e_906a_48ae_1d19,
        }
    );
    assert_eq!(
        hash_bytes(&[0x08, 0x00]),
        Hash128 {
            h1: 0x14ba_c8a2_2eeb_28fe,
            h2: 0x04a9_c79e_11a3_63d1,
        }
    );
    assert_eq!(
        hash_bytes(&[0x08, 0x5e]),
        Hash128 {
            h1: 0xd71c_3eda_ab65_eefd,
            h2: 0x8081_7e8a_ab48_56b6,
        }
    );
}

#[test]
fn source_bucket_index_uses_hash_stride_and_width() {
    let sketch = CmsSketch::new(3, 8);
    let hash = Hash128 { h1: 7, h2: 13 };
    assert_eq!(sketch.bucket_index(0, hash), 7);
    assert_eq!(sketch.bucket_index(1, hash), 4);
    assert_eq!(sketch.bucket_index(2, hash), 1);
    assert_eq!(sketch.memory_usage(), 3 * 8 * 4);
}

#[test]
fn source_query_boundary_and_default_value_are_byte_scoped() {
    let mut sketch = CmsSketch::new(5, 2_048);
    assert_eq!(sketch.query_bytes(b"missing"), 0);

    sketch.insert_bytes_by_count(b"foo", 3);
    assert_eq!(sketch.total_count(), 3);
    assert_eq!(sketch.query_bytes(b"foo"), 3);

    // The Go query path keeps a zero estimate at zero before consulting
    // considerDefVal; the fallback is only used after noise elimination has
    // produced a positive estimate.
    sketch.set_default_value(9);
    assert_eq!(sketch.query_bytes(b"missing"), 0);

    sketch.calc_default_value_for_analyze(0);
    assert_eq!(sketch.default_value(), 3);
    sketch.sub_hashed(hash_bytes(b"foo"), 1);
    assert_eq!(sketch.total_count(), 2);
    assert_eq!(sketch.query_bytes(b"foo"), 2);
}

#[test]
fn source_merge_requires_equal_dimensions_and_clone_is_deep() {
    let mut destination = CmsSketch::new(5, 2_048);
    destination.insert_bytes_by_count(b"foo", 2);
    let clone = destination.clone();

    let mut source = CmsSketch::new(5, 2_048);
    source.insert_bytes_by_count(b"foo", 3);
    destination.merge(&source).expect("equal dimensions merge");
    assert_eq!(destination.query_bytes(b"foo"), 5);
    assert_eq!(clone.query_bytes(b"foo"), 2);

    let incompatible = CmsSketch::new(4, 2_048);
    assert_eq!(
        destination.merge(&incompatible),
        Err(MergeError {
            destination_depth: 5,
            destination_width: 2_048,
            source_depth: 4,
            source_width: 2_048,
        })
    );

    assert_eq!(CmsSketch::try_new(0, 2_048).expect("zero depth").depth(), 0);
    assert_eq!(CmsSketch::try_new(5, 1).expect("unit width").width(), 1);
}

#[test]
fn source_topn_sort_lookup_range_and_clone_shape() {
    let mut topn = TopN::new(3);
    topn.append(b"b", 2);
    topn.append(b"a", 4);
    topn.append(b"c", 8);
    topn.sort();

    assert_eq!(topn.entries()[0].encoded, b"a");
    assert_eq!(topn.query_bytes(b"b"), Some(2));
    assert_eq!(topn.query_bytes(b"missing"), None);
    assert_eq!(topn.lower_bound(b"bb"), 2);
    assert_eq!(topn.between_count(b"a", b"c"), 6);
    assert_eq!(topn.total_count(), 14);
    assert_eq!(topn.min_count(), 2);

    let clone = topn.clone();
    topn.append(b"d", 16);
    assert_eq!(clone.total_count(), 14);
    assert_eq!(clone.query_bytes(b"a"), Some(4));
}

#[test]
fn source_topn_precedes_sketch_and_falls_back_to_sketch() {
    let mut sketch = CmsSketch::new(5, 2_048);
    sketch.insert_bytes(b"fallback");
    let mut topn = TopN::new(1);
    topn.append(b"top", 17);
    topn.sort();

    assert_eq!(sketch.query_with_topn(Some(&topn), b"top"), 17);
    assert_eq!(sketch.query_with_topn(Some(&topn), b"fallback"), 1);
    assert_eq!(sketch.query_with_topn(None, b"fallback"), 1);
}

#[test]
fn integer_datum_encode_and_query_match_codec_encode_value() {
    assert_eq!(
        encode_integer_datum_value(&Datum::new_int(0)).unwrap(),
        [8, 0]
    );
    assert_eq!(
        encode_integer_datum_value(&Datum::new_int(-1)).unwrap(),
        [8, 1]
    );
    assert_eq!(
        encode_integer_datum_value(&Datum::new_int(1)).unwrap(),
        [8, 2]
    );
    assert_eq!(
        encode_integer_datum_value(&Datum::new_uint(300)).unwrap(),
        [9, 0xac, 0x02]
    );
    assert_eq!(
        encode_integer_datum_value(&Datum::new_int(i64::MIN)).unwrap(),
        [8, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01]
    );
    assert_eq!(
        encode_integer_datum_value(&Datum::new_int(i64::MAX)).unwrap(),
        [8, 0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01]
    );
    assert_eq!(
        encode_integer_datum_value(&Datum::new_uint(u64::MAX)).unwrap(),
        [9, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01]
    );
    assert!(encode_integer_datum_value(&Datum::Null).is_err());

    let encoded = encode_integer_datum_value(&Datum::new_int(-7)).unwrap();
    let mut sketch = CmsSketch::new(5, 2_048);
    sketch.insert_bytes_by_count(&encoded, 3);
    assert_eq!(
        sketch
            .query_integer_datum(None, &Datum::new_int(-7))
            .unwrap(),
        3
    );
    let mut topn = TopN::new(1);
    topn.append(&encoded, 11);
    topn.sort();
    assert_eq!(
        sketch
            .query_integer_datum(Some(&topn), &Datum::new_int(-7))
            .unwrap(),
        11
    );
}

#[test]
fn test_cmsketch() {
    // Exact Go rand.NewZipf seeds, population and error envelopes from
    // TestCMSketch. Integer values traverse codec.EncodeValue-compatible
    // bytes on both insertion and QueryValue.
    for (factor, maximum_error) in [(1.1, 3_u64), (2.0, 24), (3.0, 63)] {
        let (mut left, mut left_counts) = build_seeded_zipf_sketch(0, 100_000, 1_000_000, factor);
        assert!(average_absolute_integer_error(&left, None, &left_counts) <= maximum_error);

        let (right, right_counts) = build_seeded_zipf_sketch(1, 100_000, 1_000_000, factor);
        assert!(average_absolute_integer_error(&right, None, &right_counts) <= maximum_error);

        left.merge(&right).expect("equal dimensions merge");
        for (value, count) in right_counts {
            *left_counts.entry(value).or_insert(0) += count;
        }
        assert!(
            average_absolute_integer_error(&left, None, &left_counts)
                < maximum_error.wrapping_mul(2)
        );
    }
}

#[test]
fn test_cmsketch_coding() {
    let mut sketch = CmsSketch::new(5, 2_048);
    for row in 0..sketch.depth() {
        for column in 0..sketch.width() {
            assert!(sketch.set_counter_at(row, column, u32::MAX));
        }
    }
    sketch.set_total_count(2_048 * u64::from(u32::MAX));
    sketch.set_default_value(0);
    let bytes = encode_cmsketch_without_topn(Some(&sketch))
        .expect("encode")
        .expect("non-nil sketch");
    assert_eq!(bytes.len(), 61_457);
    let decoded = decode_cmsketch(&bytes).expect("decode").expect("rows");
    assert_eq!(decoded, sketch);
}

#[test]
fn cmsketch_proto_small_vectors_and_nil_message_match_generated_go_wire() {
    let mut sketch = CmsSketch::new(1, 2);
    assert!(sketch.set_counter_at(0, 0, 1));
    assert!(sketch.set_counter_at(0, 1, 300));
    sketch.set_default_value(7);
    assert_eq!(
        encode_cmsketch_without_topn(Some(&sketch)).unwrap(),
        Some(vec![0x0a, 0x05, 0x08, 0x01, 0x08, 0xac, 0x02, 0x18, 0x07])
    );

    let mut topn = TopN::new(1);
    topn.append(b"a", 9);
    topn.sort();
    assert_eq!(
        encode_cmsketch_and_topn(Some(&sketch), Some(&topn)),
        Some(vec![
            0x0a, 0x05, 0x08, 0x01, 0x08, 0xac, 0x02, 0x12, 0x05, 0x0a, 0x01, b'a', 0x10, 0x09,
            0x18, 0x07,
        ])
    );
    assert_eq!(encode_cmsketch_and_topn(None, None), Some(vec![0x18, 0x00]));
    assert_eq!(
        decode_cmsketch_and_embedded_topn(&[0x18, 0x00]).unwrap(),
        (None, None)
    );
}

#[test]
fn test_cmsketch_topn() {
    // Exact TestCMSketchTopN seeds, first-1000 sample, million-row stream and
    // all four source accuracy envelopes.
    for (factor, maximum_error) in [(1.000_000_1, 30_u64), (1.1, 30), (2.0, 89), (5.0, 208)] {
        let mut zipf = GoZipf::new(0, factor, 1.0, 1_000_000);
        let mut counts = HashMap::new();
        let mut sample = Vec::with_capacity(1_000);
        for index in 0..1_000_000_u64 {
            let value = zipf.next() as i64;
            *counts.entry(value).or_insert(0) += 1;
            if index < 1_000 {
                sample.push(
                    encode_integer_datum_value(&Datum::new_int(value))
                        .expect("integer EncodeValue"),
                );
            }
        }
        let (sketch, topn, _, _) =
            new_cmsketch_and_topn(5, 2_048, &sample, 20, 1_000_000).expect("sample builds");
        assert!(topn.as_ref().is_some_and(|topn| topn.num() <= 40));
        assert!(average_absolute_integer_error(&sketch, topn.as_ref(), &counts) <= maximum_error);
    }
}

#[test]
fn test_cmsketch_topn_unique_data() {
    let sample: Vec<Vec<u8>> = (0..1_000_i64)
        .map(|value| {
            encode_integer_datum_value(&Datum::new_int(value)).expect("integer EncodeValue")
        })
        .collect();
    let (sketch, topn, ndv, scale) =
        new_cmsketch_and_topn(5, 2_048, &sample, 20, 1_000_000).expect("unique sample builds");
    assert_eq!(ndv, 1_000_000);
    assert_eq!(scale, 1);
    assert_eq!(sketch.default_value(), 1);
    assert!(topn.is_none());
    let mut absolute_error = 0_u64;
    for value in 0..1_000_000_i64 {
        let estimate = sketch
            .query_integer_datum(None, &Datum::new_int(value))
            .unwrap();
        absolute_error = absolute_error.wrapping_add(estimate.abs_diff(1));
    }
    // Go's averageAbsoluteError performs integer division after summing all
    // errors; individual collisions (for example value 47 -> 2) are expected.
    assert_eq!(absolute_error / 1_000_000, 0);
}

#[test]
fn test_cmsketch_coding_topn() {
    let mut sketch = CmsSketch::new(5, 2_048);
    for row in 0..sketch.depth() {
        for column in 0..sketch.width() {
            assert!(sketch.set_counter_at(row, column, u32::MAX));
        }
    }
    sketch.set_total_count(2_048 * u64::from(u32::MAX));
    let rows: Vec<_> = (0..20)
        .map(|index| (format!("{index:>20000}").into_bytes(), u64::MAX))
        .collect();
    let bytes = encode_cmsketch_without_topn(Some(&sketch))
        .expect("encode")
        .expect("non-nil");
    assert_eq!(bytes.len(), 61_457);
    let (decoded, topn) = decode_cmsketch_and_topn(Some(&bytes), &rows).expect("decode");
    assert_eq!(decoded, Some(sketch.clone()));
    assert_eq!(topn.as_ref().map(TopN::num), Some(20));
    assert!(topn
        .as_ref()
        .expect("TopN rows")
        .entries()
        .iter()
        .all(|entry| entry.count == u64::MAX && entry.encoded.len() == 20_000));
    let (empty_sketch, decoded_topn) =
        decode_cmsketch_and_topn(Some(&[]), &rows).expect("empty CMS");
    assert!(empty_sketch.is_none());
    assert_eq!(decoded_topn.as_ref().map(TopN::num), Some(20));

    let (nil_sketch, nil_topn) = decode_cmsketch_and_topn(None, &[]).expect("nil CMS and TopN");
    assert!(nil_sketch.is_none());
    assert!(nil_topn.is_none());
    let (nil_sketch_with_rows, topn_from_rows) =
        decode_cmsketch_and_topn(None, &rows).expect("nil CMS with TopN rows");
    assert!(nil_sketch_with_rows.is_none());
    assert_eq!(topn_from_rows.as_ref().map(TopN::num), Some(20));
    assert_eq!(
        encode_cmsketch_without_topn(None).expect("nil encoding"),
        None
    );

    let mut embedded_topn = TopN::new(1);
    embedded_topn.append(b"embedded", 9);
    embedded_topn.sort();
    let embedded =
        encode_cmsketch_and_topn(Some(&sketch), Some(&embedded_topn)).expect("full proto");
    let (decoded_embedded, decoded_embedded_topn) =
        decode_cmsketch_and_embedded_topn(&embedded).expect("decode full proto");
    assert_eq!(decoded_embedded, Some(sketch));
    assert_eq!(decoded_embedded_topn, Some(embedded_topn));
}

#[test]
fn test_sort_topn_meta() {
    let mut entries = vec![
        TopNEntry {
            encoded: b"a".to_vec(),
            count: 1,
        },
        TopNEntry {
            encoded: b"b".to_vec(),
            count: 2,
        },
    ];
    sort_topn_meta(&mut entries);
    assert_eq!(entries[0].count, 2);
    assert_eq!(
        topn_meta_compare(&entries[0], &entries[1]),
        std::cmp::Ordering::Less
    );
}

#[test]
fn test_topn_scale() {
    let base_entries: Vec<_> = (0_u64..20)
        .map(|index| TopNEntry {
            encoded: index.to_be_bytes().to_vec(),
            count: index * 1_001,
        })
        .collect();
    let original = base_entries.iter().map(|entry| entry.count).sum::<u64>() as f64;
    for factor in [0.9999_f64, 1.00001, 1.9999, 4.9999, 5.001, 9.99] {
        let mut entries = base_entries.clone();
        for entry in &mut entries {
            entry.count = (entry.count as f64 * factor) as u64;
        }
        let scaled = entries.iter().map(|entry| entry.count).sum::<u64>() as f64;
        assert!(((scaled - original * factor) / (original * factor)).abs() < 0.0001);
    }
}

#[test]
fn source_topn_merge_ranking_and_spill() {
    let mut left = TopN::new(2);
    left.append(b"a", 3);
    left.append(b"b", 1);
    left.sort();
    let mut right = TopN::new(2);
    right.append(b"a", 4);
    right.append(b"c", 5);
    right.sort();
    let (merged, remainder) = merge_topn(&[Some(&left), Some(&right)], 2);
    let merged = merged.expect("non-empty merge");
    assert_eq!(merged.query_bytes(b"a"), Some(7));
    assert_eq!(merged.query_bytes(b"c"), Some(5));
    assert_eq!(remainder.len(), 1);
    let (split, remainder) = get_merged_topn_from_sorted_slice(
        vec![
            TopNEntry {
                encoded: b"a".to_vec(),
                count: 1,
            },
            TopNEntry {
                encoded: b"b".to_vec(),
                count: 2,
            },
        ],
        1,
    );
    assert_eq!(split.expect("split").query_bytes(b"b"), Some(2));
    assert_eq!(remainder[0].encoded, b"a");
}

#[test]
fn source_topn_zero_limit_clears_destination_before_spill() {
    let mut destination = TopN::new(1);
    destination.append(b"a", 3);
    destination.sort();
    let mut source = TopN::new(1);
    source.append(b"b", 4);
    source.sort();
    let mut sketch = CmsSketch::new(5, 2_048);

    let remainder = merge_topn_and_update_cmsketch(&mut destination, &source, &mut sketch, 0);

    assert_eq!(destination.num(), 0);
    assert_eq!(remainder.len(), 2);
    assert_eq!(sketch.query_bytes(b"a"), 3);
    assert_eq!(sketch.query_bytes(b"b"), 4);
}

#[test]
fn source_optional_topn_stabilization_orders_equal_counts_by_bytes() {
    let sample = vec![
        b"c".to_vec(),
        b"a".to_vec(),
        b"b".to_vec(),
        b"c".to_vec(),
        b"a".to_vec(),
        b"b".to_vec(),
    ];
    let (_, topn, _, _) =
        new_cmsketch_and_topn_with_tie_stabilization(5, 2_048, &sample, 1, 60, true)
            .expect("sample builds");
    let topn = topn.expect("threshold enables TopN");
    assert_eq!(topn.entries()[0].encoded, b"a");
    assert_eq!(topn.entries()[1].encoded, b"b");
}

#[test]
fn source_decoder_accepts_packed_counter_wire_form() {
    // CMSketch{rows: [{counters: [1, 2, 300]}], default_value: 7}.
    let packed = [0x0a, 0x06, 0x0a, 0x04, 0x01, 0x02, 0xac, 0x02, 0x18, 0x07];
    let sketch = decode_cmsketch(&packed)
        .expect("valid packed protobuf")
        .expect("one row");
    assert_eq!((sketch.width(), sketch.depth()), (3, 1));
    assert_eq!(sketch.counter_at(0, 0), Some(1));
    assert_eq!(sketch.counter_at(0, 1), Some(2));
    assert_eq!(sketch.counter_at(0, 2), Some(300));
    assert_eq!(sketch.total_count(), 303);
    assert_eq!(sketch.default_value(), 7);
}
