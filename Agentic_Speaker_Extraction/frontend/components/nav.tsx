"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

const tabs = [
  { href: "/dashboard", label: "Dashboard" },
  { href: "/scraper", label: "Scraper" },
  { href: "/conferences", label: "Conferences" },
  { href: "/physicians", label: "Physicians" },
];

export function Nav() {
  const pathname = usePathname();

  return (
    <nav className="nav-shell">
      {tabs.map((tab) => {
        const active = pathname === tab.href || pathname.startsWith(`${tab.href}/`);
        return (
          <Link className={active ? "tab active" : "tab"} key={tab.href} href={tab.href}>
            {tab.label}
          </Link>
        );
      })}
    </nav>
  );
}
