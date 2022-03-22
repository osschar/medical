#include "RtClasses.h"

#include <TH1.h>

#include <cstdio>

//==============================================================================
// RtBase
//==============================================================================

void RtSetBase::attach_or_create(bool attach)
{
  if (attach)
  {
    /* TBranch *seq_br = */ m_tree->SetBranchAddress("SEQID", &m_seqid);
  }
  else
  {
    m_tree->Branch("SEQID", &m_seqid);
  }

  for (auto & f : m_rt_files)
  {
    f->attach_or_create(m_tree, attach);
  }
}  

RtSetBase::RtSetBase()
{
  // ----------------------------------------------------------------
  // Initialize table vector m_rt_files
  RT_SET_INIT_TABLE_VECTOR;

  printf("RtSetBase::RtSetBase Initialized %d tables\n", num_files());
}

RtSetBase::~RtSetBase()
{
  m_file->Close();
  delete m_file;
}

//==============================================================================
// RtSetCreator
//==============================================================================

RtSetCreator::RtSetCreator(const char* fname) :
  RtSetBase()
{
  m_file = TFile::Open(fname, "RECREATE");
  m_tree = new TTree("T", "Epic all RTs Merged");

  attach_or_create(false);
}

RtSetCreator::~RtSetCreator()
{
  m_tree->Write();
}
  
void RtSetCreator::FillLoop()
{
  seq_id_t sid;

  m_rt_files[0]->InitReadFromMainBranch();

  while ((sid = m_rt_files[0]->FillMainBranch()) > seq_id_t(-1))
  {
    printf("Processing sid %lld\n", sid);

    m_seqid = sid;

    for (int ii = 1; ii < num_files(); ++ii)
    {
      m_rt_files[ii]->FillBranch(sid);
    }

    m_tree->Fill();
  }
}

//==============================================================================
// RtSet
//==============================================================================

RtSet::RtSet(const char* fname) :
  RtSetBase()
{
  m_file = TFile::Open(fname, "READONLY");
  m_tree = (TTree*) m_file->Get("T");
    
  attach_or_create(true);
}

void RtSet::SetupIndexRangeHistos(int max_range_for_index)
{
  h_index_range = new TH1I("IndexRangeSize", "Number of days within which 4 measurements are available",
                           11 + max_range_for_index, -10.5, max_range_for_index + 0.5);;

  hv_sec_delta_histos.resize(m_sec_acc_list.size());

  for (int i = 0; i < (int) m_sec_acc_list.size(); ++i)
  {
    hv_sec_delta_histos[i] = new TH1I(TString::Format("IndexDelta%s", m_sec_acc_list[i]->get_name().Data()),
                                      TString::Format("Delta from IndexRange for %s", m_sec_acc_list[i]->get_name().Data()),
                                      200, -1000, 1000);
  }
}

//------------------------------------------------------------------------------

int RtSet::GetTreeEntry(Long64_t entry, int max_range_for_index)
{
  m_tree->GetEntry(entry);

  m_index_results.clear();

  reset_selected_index_for_all_files();

  int best_index_delta = select_index_event(m_prim_acc_list, max_range_for_index, m_index_results);
  h_index_range->Fill(best_index_delta);

  if (best_index_delta >= 0)
  {
    select_closest_entries(m_index_results[0], m_sec_acc_list);

    apply_selected_index_for_all_files();
    
    for (int i = 0; i < (int) m_sec_acc_list.size(); ++i)
    {
      int sel_index = m_sec_acc_list[i]->get_selected_index();
      int delta = sel_index >= 0 ? m_index_results[0].delta_from_index_range( m_sec_acc_list[i]->get_data(sel_index) ) : -999;
      hv_sec_delta_histos[i]->Fill(delta);
    }
  }

  return best_index_delta;
}

#ifdef DEMO_EPIC_v6
// Example of custom search for entries to select.
// Expects specifically names demographics variable from Epicv6, I think.
int RtSet::GetTreeEntryDemo30Hack(Long64_t entry)
{
  const int max_off_from_demo   = 30;
  const int max_range_for_index = 2 * max_off_from_demo + 1;

  m_tree->GetEntry(entry);

  m_index_results.clear();

  reset_selected_index_for_all_files();

  // 1. Sort Demo
  // for (auto &d : demo_v) printf("%d ", d.Date); printf("\n");
  std::sort(demo_v.begin(), demo_v.end(), [](const Demo& a, const Demo& b) -> bool { return a.Date < b.Date; });
  // for (auto &d : demo_v) printf("%d ", d.Date); printf("\n");
  // Hack, keep first one only
  demo_v.resize(1);

  int best_index_delta = select_index_event(m_prim_acc_list, max_range_for_index, m_index_results);

  // 2. Make sure selected index event is within +-30 days of demo[0]
  int si = -1;
  for (int i = 0; i < (int) m_index_results.size(); ++i)
  {
    if (m_index_results[i].f_min_day >= demo_v[0].Date - 30 &&
        m_index_results[i].f_max_day <= demo_v[0].Date + 30)
    {
      if (i > 0)
      {
        best_index_delta = m_index_results[i].f_max_day - m_index_results[i].f_min_day;
        m_index_results[0] = m_index_results[i];
      }
      break;
    }
  }

  h_index_range->Fill(best_index_delta);

  if (best_index_delta >= 0)
  {
    select_closest_entries(m_index_results[0], m_sec_acc_list);

    apply_selected_index_for_all_files();
    
    for (int i = 0; i < (int) m_sec_acc_list.size(); ++i)
    {
      int sel_index = m_sec_acc_list[i]->get_selected_index();
      int delta = sel_index >= 0 ? m_index_results[0].delta_from_index_range( m_sec_acc_list[i]->get_data(sel_index) ) : -999;
      hv_sec_delta_histos[i]->Fill(delta);
    }
  }

  return best_index_delta;
}
#endif

//------------------------------------------------------------------------------

// returns number of tables with no entries.
int RtSet::select_and_apply_first_entry_for_all_files()
{
  int n_missing = 0;
  for (auto & ff : m_rt_files)
  {
    if (ff->rt_present())
      ff->set_selected_index(0);
    else {
      ff->reset_selected_index();
      ++n_missing;
    }
    ff->apply_selected_index();
  }
  return n_missing;
}

//------------------------------------------------------------------------------

void RtSet::reset_selected_index_for_all_files()
{
  for (auto & ff : m_rt_files)
    ff->reset_selected_index();
}

void RtSet::apply_selected_index_for_all_files()
{
  for (auto & ff : m_rt_files)
    ff->apply_selected_index();
}

//------------------------------------------------------------------------------

void RtSet::print_acc_list(AccessList_t& accl)
{
  int max_size = 0;
  for (auto & acch : accl)
  {
    max_size = std::max(max_size, acch->get_size());
  }
  printf("print_acc_list max_size = %d\n", max_size);

  printf("%-4s ", "idx");
  for (auto & acch : accl)
  {
    printf("%-7s ", acch->get_name().Data());
  }
  printf("\n");

  for (int i = 0; i < max_size; ++i)
  {
      printf("%-4d ", i);

      for (auto & acch : accl)
      {
        if (i < acch->get_size())
          printf("%-7d ", acch->get_data(i));
        else
          printf("%7s ", " ");
      }
      printf("\n");
  }
}

//------------------------------------------------------------------------------

int RtSet::recurse_select_index_event(AccessList_t& accl, const int max_delta,
                                      std::vector<int>& maxv, const int N,
                                      int pos, const IndexEventRange& res,
                                      IndexEventRange_v& results)
{
  int sum = 0;

  for (int i = 0; i < maxv[pos]; ++i)
  {
    if ( ! accl[pos]->test_cut(i)) continue;

    if (res.consider_day(accl[pos]->get_data(i)) <= max_delta)
    {
      IndexEventRange res2(res);

      res2.set_at_pos(pos, i, accl[pos]->get_data(i));

      if (pos + 1 < N)
      {
        sum += recurse_select_index_event(accl, max_delta, maxv, N, pos + 1, res2, results);
      }
      else
      {
        /*
        if (! results.empty() && res2.days() < results[0].days())
        {
          results.emplace_back(results[0]);
          results[0] = res2;
        }
        else
        {
          results.emplace_back(res2);
        }
        */

        results.emplace_back(res2);

        ++sum;
      }
    }
  }

  return sum;
}

int RtSet::select_index_event(AccessList_t& accl, const int max_delta, IndexEventRange_v& results)
{
  // returns number of days for closest event or -9 if nothing is found within max_delta.

  const int N = accl.size();

  std::vector<int> maxv(N);
  for (int i = 0; i < N; ++i)
  {
    maxv[i] = accl[i]->get_size();
  }

  int sum = 0;

  for (int i = 0; i < maxv[0]; ++i)
  {
    if ( ! accl[0]->test_cut(i)) continue;

    IndexEventRange res(N);
    res.set_first(i, accl[0]->get_data(i));

    if (N > 1)
      sum += recurse_select_index_event(accl, max_delta, maxv, N, 1, res, results);
    else
      results.emplace_back(res);
  }

  if ( ! results.empty())
  {
    for (int i = 0; i < N; ++i) accl[i]->set_selected_index( results[0].f_indices[i] );
  }
  return results.empty() ? -9 : results[0].days();
}

//------------------------------------------------------------------------------

void RtSet::select_closest_entries(const IndexEventRange& reference, AccessList_t& accl)
{
  const int N = accl.size();

  for (int i = 0; i < N; ++i)
  {
    const int M = accl[i]->get_size();
    int min = 9999, idx = -1;
    // XXX AVI 4.3 HACK
    min = 31;
    for (int j = 0; j < M; ++j)
    {
      int d = reference.consider_day_for_closest_entry(accl[i]->get_data(j));
      if (d < min)
      {
        min = d; idx = j;
      }
    }
    accl[i]->set_selected_index(idx);
  }
}

//==============================================================================
//==============================================================================

void RtImport(const char *filename="RtSet.root")
{
  RtSetCreator c(filename);

  c.FillLoop();
}

void RtClasses()
{
  printf("RtClasses loaded.\n");
}
