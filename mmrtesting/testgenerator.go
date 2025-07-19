package mmrtesting

import (
	"crypto/sha256"
	"encoding/binary"
	"hash"
	"strings"
	"testing"
	"time"

	"github.com/datatrails/go-datatrails-merklelog/massifs"
	"github.com/datatrails/go-datatrails-merklelog/massifs/storage"
	"github.com/google/uuid"
)

const (
	jitterMultipler = 2
)

func HashWriteUint64(hasher hash.Hash, value uint64) {
	b := [8]byte{}
	binary.BigEndian.PutUint64(b[:], value)
	hasher.Write(b[:])
}

// TestGenerator creates random values of various sorts for testing. Seeded so that from run to
// run the values can be the same. Intended for white box tests that benefit from a
// large volume of synthetic data.
type TestGenerator struct {
	Cfg       *TestOptions
	T         *testing.T
	StartTime time.Time
	LastTime  time.Time
}

// AddLeafArgs is the return value of all LeafGenerator implementations. It
// carries just enough to successfully call AddLeafEntry on a MassifContext The
// leaf generator is free to decide on appropriate values
type AddLeafArgs struct {
	ID    uint64
	AppID []byte
	Value []byte
	LogID []byte
}

type LeafGenerator func(base, i uint64) AddLeafArgs

func MMRTestingGenerateNumberedLeaf(base, i uint64) AddLeafArgs {
	h := sha256.New()
	HashWriteUint64(h, base+i)

	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, base+i)
	return AddLeafArgs{
		ID:    0,
		AppID: b,
		Value: h.Sum(nil),
	}
}

// NewTestGenerator creates a deterministic, but random looking, test data generator.
// Given the same seed, the series of data generated on different runs is identical.
// This means that we generate valid values for things like uuid based
// identities and simulated time stamps, but the log telemetry from successive runs will
// be usefuly stable.
func NewTestGenerator(t *testing.T, cfg *TestOptions, opts ...Option) TestGenerator {

	g := TestGenerator{}
	g.Init(t, cfg, opts...)
	return g
}

// Using the
func (g *TestGenerator) Init(t *testing.T, cfg *TestOptions, opts ...Option) {
	if cfg == nil {
		cfg = &TestOptions{}
	}
	for _, opt := range opts {
		opt(cfg)
	}
	g.Cfg = cfg
	g.T = t
	g.StartTime = time.UnixMilli(cfg.StartTimeMS)
	g.LastTime = time.UnixMilli(cfg.StartTimeMS)
}

func (g *TestGenerator) GenerateNumberedLeafBatch(logID storage.LogID, base, count uint64) []AddLeafArgs {
	h := sha256.New()
	return g.GenerateLeafBatch(base, count, func(base, i uint64) AddLeafArgs {
		h.Reset()
		HashWriteUint64(h, base+i)
		args := g.Cfg.LeafGenerator(base, i)
		return AddLeafArgs{
			ID:    args.ID,
			AppID: args.AppID,
			Value: h.Sum(nil),
			LogID: logID,
		}
	})
}

func (g *TestGenerator) GenerateLeafBatch(base, count uint64, gf LeafGenerator) []AddLeafArgs {
	indexedLeaves := make([]AddLeafArgs, 0, count)
	for i := range count {
		args := gf(base, i)
		indexedLeaves = append(indexedLeaves, args)
	}
	return indexedLeaves
}

func (g *TestGenerator) PadWithLeafEntries(data []byte, n int) []byte {
	return g.PadWithNumberedLeaves(data, 0, n)
}

func (g *TestGenerator) PadWithNumberedLeaves(data []byte, first, n int) []byte {
	if n == 0 {
		return data
	}
	values := make([]byte, massifs.ValueBytes*n)
	for i := range n {
		binary.BigEndian.PutUint32(values[i*massifs.ValueBytes+massifs.ValueBytes-4:i*massifs.ValueBytes+massifs.ValueBytes], uint32(first+i))
	}
	return append(data, values...)
}

func (g *TestGenerator) SinceLastJitter(noUpdate ...bool) time.Time {
	// the * 2 puts the normal mid point on the ideal rate.
	ts := g.LastTime.Add(time.Millisecond * time.Duration(g.Float32()*1000.0/float32(g.Cfg.EventRate*jitterMultipler)))
	if len(noUpdate) == 0 || !noUpdate[0] {
		g.LastTime = ts
	}
	return ts
}

func (g *TestGenerator) SinceJitter(lastTime time.Time) time.Time {
	return lastTime.Add(time.Millisecond * time.Duration(g.Float32()*1000.0/float32(g.Cfg.EventRate)))
}

func (g *TestGenerator) NewLogID() storage.LogID {
	id, err := g.NewRandomUUID()
	if err != nil {
		g.T.Fatalf("failed to generate uuid")
		return nil
	}
	return id[:]
}

func (g *TestGenerator) NewRandomUUIDString(t *testing.T) string {
	id, err := g.NewRandomUUID()
	if err != nil {
		t.Fatalf("failed to generate uuid")
		return ""
	}
	return id.String()
}

func (g *TestGenerator) NewRandomUUID() (uuid.UUID, error) {
	return uuid.NewRandomFromReader(g.Cfg.Rand)
}

func (g *TestGenerator) Intn(n int) int {
	return g.Cfg.Rand.Intn(n)
}

func (g *TestGenerator) Float32() float32 {
	return g.Cfg.Rand.Float32()
}

// WordList returns a list of random words selected from the table
func (g *TestGenerator) WordList(count int) []string {
	words := make([]string, 0, count)
	maxWords := len(g.Cfg.WordList)
	for range count {
		words = append(words, g.Cfg.WordList[g.Intn(maxWords)])
	}
	return words
}

func (g *TestGenerator) MultiWordString(count int) string {
	return strings.Join(g.WordList(count), " ")
}

// Just a convenient word table
func bip32WordList() []string {
	return strings.Split(strings.ReplaceAll(`
abandon ability able about above absent absorb abstract absurd abuse access
accident account accuse achieve acid acoustic acquire across act action actor
actress actual adapt add addict address adjust admit adult advance advice
aerobic affair afford afraid again age agent agree ahead aim air airport aisle
alarm album alcohol alert alien all alley allow almost alone alpha already also
alter always amateur amazing among amount amused analyst anchor ancient anger
angle angry animal ankle announce annual another answer antenna antique anxiety
any apart apology appear apple approve april arch arctic area arena argue arm
armed armor army around arrange arrest arrive arrow art artefact artist artwork
ask aspect assault asset assist assume asthma athlete atom attack attend
attitude attract auction audit august aunt author auto autumn average avocado
avoid awake aware away awesome awful awkward axis baby bachelor bacon badge bag
balance balcony ball bamboo banana banner bar barely bargain barrel base basic
basket battle beach bean beauty because become beef before begin behave behind
believe below belt bench benefit best betray better between beyond bicycle bid
bike bind biology bird birth bitter black blade blame blanket blast bleak bless
blind blood blossom blouse blue blur blush board boat body boil bomb bone bonus
book boost border boring borrow boss bottom bounce box boy bracket brain brand
brass brave bread breeze brick bridge brief bright bring brisk broccoli broken
bronze broom brother brown brush bubble buddy budget buffalo build bulb bulk
bullet bundle bunker burden burger burst bus business busy butter buyer buzz
cabbage cabin cable cactus cage cake call calm camera camp can canal cancel
candy cannon canoe canvas canyon capable capital captain car carbon card cargo
carpet carry cart case cash casino castle casual cat catalog catch category
cattle caught cause caution cave ceiling celery cement census century cereal
certain chair chalk champion change chaos chapter charge chase chat cheap check
cheese chef cherry chest chicken chief child chimney choice choose chronic
chuckle chunk churn cigar cinnamon circle citizen city civil claim clap clarify
claw clay clean clerk clever click client cliff climb clinic clip clock clog
close cloth cloud clown club clump cluster clutch coach coast coconut code
coffee coil coin collect color column combine come comfort comic common company
concert conduct confirm congress connect consider control convince cook cool
copper copy coral core corn correct cost cotton couch country couple course
cousin cover coyote crack cradle craft cram crane crash crater crawl crazy cream
credit creek crew cricket crime crisp critic crop cross crouch crowd crucial
cruel cruise crumble crunch crush cry crystal cube culture cup cupboard curious
current curtain curve cushion custom cute cycle dad damage damp dance danger
daring dash daughter dawn day deal debate debris decade december decide decline
decorate decrease deer defense define defy degree delay deliver demand demise
denial dentist deny depart depend deposit depth deputy derive describe desert
design desk despair destroy detail detect develop device devote diagram dial
diamond diary dice diesel diet differ digital dignity dilemma dinner dinosaur
direct dirt disagree discover disease dish dismiss disorder display distance
divert divide divorce dizzy doctor document dog doll dolphin domain donate
donkey donor door dose double dove draft dragon drama drastic draw dream dress
drift drill drink drip drive drop drum dry duck dumb dune during dust dutch duty
dwarf dynamic eager eagle early earn earth easily east easy echo ecology economy
edge edit educate effort egg eight either elbow elder electric elegant element
elephant elevator elite else embark embody embrace emerge emotion employ empower
empty enable enact end endless endorse enemy energy enforce engage engine
enhance enjoy enlist enough enrich enroll ensure enter entire entry envelope
episode equal equip era erase erode erosion error erupt escape essay essence
estate eternal ethics evidence evil evoke evolve exact example excess exchange
excite exclude excuse execute exercise exhaust exhibit exile exist exit exotic
expand expect expire explain expose express extend extra eye eyebrow fabric face
faculty fade faint faith fall false fame family famous fan fancy fantasy farm
fashion fat fatal father fatigue fault favorite feature february federal fee
feed feel female fence festival fetch fever few fiber fiction field figure file
film filter final find fine finger finish fire firm first fiscal fish fit
fitness fix flag flame flash flat flavor flee flight flip float flock floor
flower fluid flush fly foam focus fog foil fold follow food foot force forest
forget fork fortune forum forward fossil foster found fox fragile frame frequent
fresh friend fringe frog front frost frown frozen fruit fuel fun funny furnace
fury future gadget gain galaxy gallery game gap garage garbage garden garlic
garment gas gasp gate gather gauge gaze general genius genre gentle genuine
gesture ghost giant gift giggle ginger giraffe girl give glad glance glare glass
glide glimpse globe gloom glory glove glow glue goat goddess gold good goose
gorilla gospel gossip govern gown grab grace grain grant grape grass gravity
great green grid grief grit grocery group grow grunt guard guess guide guilt
guitar gun gym habit hair half hammer hamster hand happy harbor hard harsh
harvest hat have hawk hazard head health heart heavy hedgehog height hello
helmet help hen hero hidden high hill hint hip hire history hobby hockey hold
hole holiday hollow home honey hood hope horn horror horse hospital host hotel
hour hover hub huge human humble humor hundred hungry hunt hurdle hurry hurt
husband hybrid ice icon idea identify idle ignore ill illegal illness image
imitate immense immune impact impose improve impulse inch include income
increase index indicate indoor industry infant inflict inform inhale inherit
initial inject injury inmate inner innocent input inquiry insane insect inside
inspire install intact interest into invest invite involve iron island isolate
issue item ivory jacket jaguar jar jazz jealous jeans jelly jewel job join joke
journey joy judge juice jump jungle junior junk just kangaroo keen keep ketchup
key kick kid kidney kind kingdom kiss kit kitchen kite kitten kiwi knee knife
knock know lab label labor ladder lady lake lamp language laptop large later
latin laugh laundry lava law lawn lawsuit layer lazy leader leaf learn leave
lecture left leg legal legend leisure lemon lend length lens leopard lesson
letter level liar liberty library license life lift light like limb limit link
lion liquid list little live lizard load loan lobster local lock logic lonely
long loop lottery loud lounge love loyal lucky luggage lumber lunar lunch luxury
lyrics machine mad magic magnet maid mail main major make mammal man manage
mandate mango mansion manual maple marble march margin marine market marriage
mask mass master match material math matrix matter maximum maze meadow mean
measure meat mechanic medal media melody melt member memory mention menu mercy
merge merit merry mesh message metal method middle midnight milk million mimic
mind minimum minor minute miracle mirror misery miss mistake mix mixed mixture
mobile model modify mom moment monitor monkey monster month moon moral more
morning mosquito mother motion motor mountain mouse move movie much muffin mule
multiply muscle museum mushroom music must mutual myself mystery myth naive name
napkin narrow nasty nation nature near neck need negative neglect neither nephew
nerve nest net network neutral never news next nice night noble noise nominee
noodle normal north nose notable note nothing notice novel now nuclear number
nurse nut oak obey object oblige obscure observe obtain obvious occur ocean
october odor off offer office often oil okay old olive olympic omit once one
onion online only open opera opinion oppose option orange orbit orchard order
ordinary organ orient original orphan ostrich other outdoor outer output outside
oval oven over own owner oxygen oyster ozone pact paddle page pair palace palm
panda panel panic panther paper parade parent park parrot party pass patch path
patient patrol pattern pause pave payment peace peanut pear peasant pelican pen
penalty pencil people pepper perfect permit person pet phone photo phrase
physical piano picnic picture piece pig pigeon pill pilot pink pioneer pipe
pistol pitch pizza place planet plastic plate play please pledge pluck plug
plunge poem poet point polar pole police pond pony pool popular portion position
possible post potato pottery poverty powder power practice praise predict prefer
prepare present pretty prevent price pride primary print priority prison private
prize problem process produce profit program project promote proof property
prosper protect proud provide public pudding pull pulp pulse pumpkin punch pupil
puppy purchase purity purpose purse push put puzzle pyramid quality quantum
quarter question quick quit quiz quote rabbit raccoon race rack radar radio rail
rain raise rally ramp ranch random range rapid rare rate rather raven raw razor
ready real reason rebel rebuild recall receive recipe record recycle reduce
reflect reform refuse region regret regular reject relax release relief rely
remain remember remind remove render renew rent reopen repair repeat replace
report require rescue resemble resist resource response result retire retreat
return reunion reveal review reward rhythm rib ribbon rice rich ride ridge rifle
right rigid ring riot ripple risk ritual rival river road roast robot robust
rocket romance roof rookie room rose rotate rough round route royal rubber rude
rug rule run runway rural sad saddle sadness safe sail salad salmon salon salt
salute same sample sand satisfy satoshi sauce sausage save say scale scan scare
scatter scene scheme school science scissors scorpion scout scrap screen script
scrub sea search season seat second secret section security seed seek segment
select sell seminar senior sense sentence series service session settle setup
seven shadow shaft shallow share shed shell sheriff shield shift shine ship
shiver shock shoe shoot shop short shoulder shove shrimp shrug shuffle shy
sibling sick side siege sight sign silent silk silly silver similar simple since
sing siren sister situate six size skate sketch ski skill skin skirt skull slab
slam sleep slender slice slide slight slim slogan slot slow slush small smart
smile smoke smooth snack snake snap sniff snow soap soccer social sock soda soft
solar soldier solid solution solve someone song soon sorry sort soul sound soup
source south space spare spatial spawn speak special speed spell spend sphere
spice spider spike spin spirit split spoil sponsor spoon sport spot spray spread
spring spy square squeeze squirrel stable stadium staff stage stairs stamp stand
start state stay steak steel stem step stereo stick still sting stock stomach
stone stool story stove strategy street strike strong struggle student stuff
stumble style subject submit subway success such sudden suffer sugar suggest
suit summer sun sunny sunset super supply supreme sure surface surge surprise
surround survey suspect sustain swallow swamp swap swarm swear sweet swift swim
swing switch sword symbol symptom syrup system table tackle tag tail talent talk
tank tape target task taste tattoo taxi teach team tell ten tenant tennis tent
term test text thank that theme then theory there they thing this thought three
thrive throw thumb thunder ticket tide tiger tilt timber time tiny tip tired
tissue title toast tobacco today toddler toe together toilet token tomato
tomorrow tone tongue tonight tool tooth top topic topple torch tornado tortoise
toss total tourist toward tower town toy track trade traffic tragic train
transfer trap trash travel tray treat tree trend trial tribe trick trigger trim
trip trophy trouble truck true truly trumpet trust truth try tube tuition tumble
tuna tunnel turkey turn turtle twelve twenty twice twin twist two type typical
ugly umbrella unable unaware uncle uncover under undo unfair unfold unhappy
uniform unique unit universe unknown unlock until unusual unveil update upgrade
uphold upon upper upset urban urge usage use used useful useless usual utility
vacant vacuum vague valid valley valve van vanish vapor various vast vault
vehicle velvet vendor venture venue verb verify version very vessel veteran
viable vibrant vicious victory video view village vintage violin virtual virus
visa visit visual vital vivid vocal voice void volcano volume vote voyage wage
wagon wait walk wall walnut want warfare warm warrior wash wasp waste water wave
way wealth weapon wear weasel weather web wedding weekend weird welcome west wet
whale what wheat wheel when where whip whisper wide width wife wild will win
window wine wing wink winner winter wire wisdom wise wish witness wolf woman
wonder wood wool word work world worry worth wrap wreck wrestle wrist write
wrong yard year yellow you young youth zebra zero zone zoo`, "\n", " "), " ")
}
