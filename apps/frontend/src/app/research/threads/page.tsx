import Link from "next/link";
import { ThreadsEDA } from "@/features/research/ThreadsEDA";

export default function ThreadsResearchPage() {
  return <main className="tracker-main"><Link href="/">DSP391m tracker</Link><ThreadsEDA /></main>;
}
