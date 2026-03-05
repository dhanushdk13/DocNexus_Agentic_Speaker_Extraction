import type { Metadata } from "next";
import { DM_Sans, Manrope } from "next/font/google";

import "./globals.css";
import { Nav } from "../components/nav";

const heading = Manrope({ subsets: ["latin"], variable: "--font-heading" });
const body = DM_Sans({ subsets: ["latin"], variable: "--font-body" });

export const metadata: Metadata = {
  title: "DocNexus",
  description: "Conference speaker extraction and physician intelligence workspace.",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body className={`${heading.variable} ${body.variable}`}>
        <div className="page-bg" />
        <div className="container app-shell">
          <header className="hero brand-hero">
            <div className="brand-lockup">
              <div className="brand-mark">D</div>
              <div>
                <p className="brand-name">DocNexus</p>
                <p className="brand-tagline">Agentic speaker extraction from conferences</p>
              </div>
            </div>
          </header>
          <Nav />
          <main>{children}</main>
        </div>
      </body>
    </html>
  );
}
