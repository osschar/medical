#ifndef RTCLASSES_H
#define RTCLASSES_H

#include "TFile.h"
#include "TTree.h"

#include "TPRegexp.h"

class TH1;

#include <functional>
#include <vector>

#include "structs.h"


//==============================================================================
// RtFileBase
//==============================================================================

class RtFileBase
{
public:
  virtual void attach_or_create(TTree *tree, bool attach) = 0;

  virtual void set_current_to_first_or_default() = 0;
  virtual void reset_selected_index() = 0;
  virtual void set_selected_index(int idx) = 0;
  virtual void apply_selected_index() = 0;


  virtual ~RtFileBase() {}

  virtual int InitReadFromMainBranch() = 0;
  virtual int FillMainBranch() = 0;

  virtual int FillBranch(int) = 0;
};


//==============================================================================
// RtFile
//==============================================================================

template <typename TT>
class RtFile : public RtFileBase
{
  FILE       *m_fp = 0;
  TBranch    *m_branch;
  TBranch    *m_present;
  
public: 
  std::vector<TT> *m_rt_vec;
  TT               m_rt_cur;
  bool             m_rt_present;

  TPMERegexp  m_split_re;

  TString     m_name;
  TString     m_header;

  int         m_cur_seqid = 0;
  int         m_selected_index = -1; // index of vectorentry to be copied into m_rt_cur

  void next_line()
  {
    // Will be set to -1 when EOF is reached.
    m_cur_seqid = m_rt_cur.ReadLine(m_fp, m_split_re);
  }

  void set_current_to_first_or_default() override
  {
    if (m_rt_present)
      m_rt_cur = (*m_rt_vec)[0];
    else
      m_rt_cur = TT();
  }

  void reset_selected_index()      override { m_selected_index = -1;  }
  void set_selected_index(int idx) override { m_selected_index = idx; }
  void apply_selected_index()      override
  {
    if (m_selected_index < 0)
      m_rt_cur = TT();
    else if (m_selected_index < (int) m_rt_vec->size())
      m_rt_cur = (*m_rt_vec)[m_selected_index];
    else
      throw std::runtime_error("Bad selected_index");
  }

public:
  RtFile(const char *id) :
    m_split_re("\\|"),
    m_name(id)
  {
    printf("Init RtFile for %s\n", id);

    m_rt_vec = new std::vector<TT>;
  }

  void attach_or_create(TTree *tree, bool attach) override
  {
    TString name_present;
    name_present.Form("%s_present", m_name.Data());

    TString stl_type; stl_type.Form("std::vector<%s>", m_name.Data());

    if (attach)
    {
      tree->SetBranchAddress(m_name, &m_rt_vec);
      tree->SetBranchAddress(name_present, &m_rt_present);

      m_rt_vec->reserve(TT::s_max_ent);
      m_rt_vec->resize(1);
      m_rt_present = true;   // Lie so address for branch copy can be obtained through DECL.
    }
    else
    {
      // WTF ... specifying buffer_size and split_level makes IT crash ...
      // m_branch = tree->Branch(name, stl_type, & m_rt_vec, 32768, 101);
      m_branch  = tree->Branch(m_name, stl_type, &m_rt_vec);
      m_present = tree->Branch(name_present, &m_rt_present);

      TString fname;
      fname.Form("RT_FILES/%s.rtt", m_name.Data());
      m_fp = fopen(fname.Data(), "r");
      if (! m_fp)
      {
        perror("Failed opening rtt file");
        exit(1);
      }
      m_header.Gets(m_fp);
    }
  }

  ~RtFile()
  {
    delete m_rt_vec;
    fclose(m_fp);
  }

  int InitReadFromMainBranch() override
  {
    // Read first entry from RTT file to bootstrap seqid handling.

    if (m_cur_seqid != 0) throw std::runtime_error("InitReadFromMainBranch() can only be called at the beginning.");

    next_line();

    return m_cur_seqid;
  }

  int FillMainBranch() override
  {
    // Read all entries for current seqid from RTT file.
    // Note that we have to over-read for one entry to detect seqid change.

    if (m_cur_seqid == -1) return -1;

    m_rt_vec->clear();
    m_rt_present = false;

    int this_seqid = m_cur_seqid;

    do
    {
      m_rt_vec->push_back(m_rt_cur);
      m_rt_present = true;

      next_line();
    }
    while (m_cur_seqid == this_seqid);

    return this_seqid;
  }

  int FillBranch(int seqid) override
  {
    m_rt_vec->clear();
    m_rt_present = false;

    int ret = -1;
    if (m_cur_seqid >= 0)
    {
      ret = 0;

      // allow for extra seqids non-main branches
      while (m_cur_seqid < seqid) next_line();

      while (m_cur_seqid == seqid)
      {
        m_rt_vec->push_back(m_rt_cur);
        m_rt_present = true;
        ++ret;
        next_line();
      }
    }

    return ret;
  }
};


//==============================================================================
// RtSetBase, RtSetCreator, RtSet
//==============================================================================

template <typename VV>
struct AccessHandleBase
{
  virtual ~AccessHandleBase() {}

  virtual const TString& get_name() = 0;

  virtual int  get_size() = 0;
  virtual VV   get_data(int i) = 0;
  virtual bool test_cut(int i) = 0;

  virtual void set_selected_index(int idx) = 0;
  virtual int  get_selected_index() const = 0;
};

template <typename TT, typename VV>
struct AccessHandle : public AccessHandleBase<VV>
{
  typedef std::function<VV   (const TT&)> acc_foo_t;
  typedef std::function<bool (const TT&)> cut_foo_t;

  RtFile<TT> &f_data;
  acc_foo_t   f_accessor;
  cut_foo_t   f_cuttor;

  AccessHandle(RtFile<TT>& d, acc_foo_t a, cut_foo_t c = [](const TT&){ return true; }) :
    f_data(d), f_accessor(a), f_cuttor(c)
  {}

  const TString& get_name() override { return f_data.m_name; }

  int  get_size()      override { return f_data.m_rt_vec->size(); }
  VV   get_data(int i) override { return f_accessor( f_data.m_rt_vec->at(i) ); }
  bool test_cut(int i) override { return f_cuttor  ( f_data.m_rt_vec->at(i) ); }

  void set_selected_index(int idx) override { f_data.m_selected_index = idx; }
  int  get_selected_index()  const override { return f_data.m_selected_index; }
};

template struct AccessHandleBase<int>;

typedef std::vector<AccessHandleBase<int>*> AccessList_t;

//------------------------------------------------------------------------------

struct IndexEventRange
{
  std::vector<int> f_indices;
  int              f_min_day =  9999;
  int              f_max_day = -9999;

  IndexEventRange() {}
  explicit IndexEventRange(int isize) : f_indices(isize) {}

  int  days() const { return f_max_day - f_min_day; }

  void set_first(int idx, int day)
  {
    f_indices[0] = idx;
    f_min_day = f_max_day = day;
  }

  int consider_day(int day) const
  {
    if (day < f_min_day) return f_max_day - day;
    if (day > f_max_day) return day - f_min_day;
    return f_max_day - f_min_day;
  }

  int consider_day_for_closest_entry(int day) const
  {
    if (day < f_min_day) return f_min_day - day;
    if (day > f_max_day) return day - f_max_day;
    return 0;
  }

  int delta_from_index_range(int day) const
  {
    if (day < f_min_day) return day - f_min_day;
    if (day > f_max_day) return day - f_max_day;
    return 0;
  }

  void set_at_pos(int pos, int idx, int day)
  {
    f_indices[pos] = idx;
    if      (day < f_min_day) f_min_day = day;
    else if (day > f_max_day) f_max_day = day;
  }
};

//using IndexEventRange_v = std::vector<IndexEventRange>;
typedef std::vector<IndexEventRange> IndexEventRange_v;

//------------------------------------------------------------------------------

class RtSetBase
{
public:
  TFile     *m_file = 0;
  TTree     *m_tree = 0;

  int                      m_seqid = -1;
  std::vector<RtFileBase*> m_rt_files;

  void attach_or_create(bool attach);

  // ----------------------------------------------------------------

  RtSetBase();
  ~RtSetBase();
};

//==============================================================================

class RtSetCreator : public RtSetBase
{
public:

  RtSetCreator(const char* fname);
  ~RtSetCreator();
  
  void FillLoop();
};


//==============================================================================

#define DECL(_cls_, _id_, _name_)                                       \
  RtFile<_cls_>&      _name_ ## _r = *((RtFile<_cls_>*)m_rt_files[_id_]); \
  std::vector<_cls_>& _name_ ## _v = * _name_ ## _r.m_rt_vec; \
  bool&               _name_ ## _p =   _name_ ## _r.m_rt_present;  \
  _cls_&              _name_       =   _name_ ## _r.m_rt_cur;


class RtSet : public RtSetBase
{
protected:
  void reset_selected_index_for_all_files();
  void apply_selected_index_for_all_files();

  int  recurse_select_index_event(AccessList_t& accl, const int max_delta,
                                 std::vector<int>& maxv, const int N,
                                 int pos, const IndexEventRange& res,
                                 IndexEventRange_v& results);

  // ----------------------------------------------------------------

  AccessList_t        m_prim_acc_list;
  AccessList_t        m_sec_acc_list;

  IndexEventRange_v   m_index_results;

  TH1                *h_index_range;
  std::vector<TH1*>   hv_sec_delta_histos;

  // ----------------------------------------------------------------
  // Shorthands for data variables

  DECL(Demo, 0, demo);
  DECL(BNP, 1, bnp);
  DECL(CBC, 2, cbc);               //*
  DECL(CMP, 3, cmp);               //*
  DECL(Device, 4, device);         //-
  DECL(ECG, 5, ecg);               //*
  DECL(Hospitalization, 6, hosp);  //-
  DECL(INR, 7, inr);
  DECL(Lactate, 8, lactate);
  DECL(Magnesium, 9, magnesium);
  DECL(Medical_hx, 10, med_hx);    //-
  DECL(TSH, 11, tsh);
  DECL(Vitals, 12, vital);
  DECL(Echo, 13, echo);

public:

  RtSet(const char* fname="Epic.root");

  void SetupIndexRangeHistos(int max_range_for_index);

  int  GetTreeEntry(Long64_t entry, int max_range_for_index);
  int  GetTreeEntryDemo30Hack(Long64_t entry);

  void print_acc_list(AccessList_t& accl);

  int  select_index_event(AccessList_t& accl, const int max_delta, IndexEventRange_v& results);

  void select_closest_entries(const IndexEventRange& reference, AccessList_t& accl);
};

#endif
